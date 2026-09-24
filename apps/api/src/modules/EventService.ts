import {
  Event,
  EventDataObject,
  EventType,
  WebhookDelivery,
  WebhookEndpointRecord,
} from '@zoneless/shared-types';
import { Database } from './Database';
import { EventModule } from './Event';
import { AccountModule } from './Account';
import { WebhookEndpointModule } from './WebhookEndpoint';
import { WebhookDeliveryModule } from './WebhookDelivery';
import { WebhookDeliveryWorker } from './WebhookDeliveryWorker';
import { GetPlatformAccountId } from './PlatformAccess';
import { GetRequestContext } from '../middleware/RequestContext';
import { Logger } from '../utils/Logger';

interface EventOptions {
  livemode?: boolean;
  apiVersion?: string;
  context?: string | null;
  previousAttributes?: Partial<EventDataObject> | null;
}

export class EventService {
  private readonly eventModule: EventModule;
  private readonly accountModule: AccountModule;
  private readonly webhookEndpointModule: WebhookEndpointModule;
  private readonly webhookDeliveryModule: WebhookDeliveryModule;
  private readonly webhookDeliveryWorker: WebhookDeliveryWorker;

  constructor(db: Database) {
    this.eventModule = new EventModule(db);
    this.accountModule = new AccountModule(db);
    this.webhookEndpointModule = new WebhookEndpointModule(db);
    this.webhookDeliveryModule = new WebhookDeliveryModule(db);
    this.webhookDeliveryWorker = new WebhookDeliveryWorker(db);
  }

  async Emit(
    type: EventType,
    account: string,
    dataObject: EventDataObject,
    options: EventOptions = {}
  ): Promise<Event> {
    const platformAccountId = await this.ResolvePlatformForEvent(
      account,
      dataObject
    );

    const reqContext = GetRequestContext();

    const endpoints =
      await this.webhookEndpointModule.GetWebhookEndpointsForEvent(
        platformAccountId,
        type
      );
    const pendingWebhooksCount = endpoints.length;

    const event = await this.eventModule.CreateEvent(
      type,
      account,
      dataObject,
      {
        ...options,
        idempotencyKey: reqContext?.idempotencyKey || null,
        requestId: reqContext?.requestId || null,
        pendingWebhooks: pendingWebhooksCount,
      }
    );

    Logger.info('Event created', {
      eventId: event.id,
      eventType: type,
      account,
      platformAccountId,
      pendingWebhooks: pendingWebhooksCount,
    });

    const deliveries = await this.PersistDeliveries(event, endpoints);

    if (deliveries.length > 0) {
      this.DeliverFirstAttempts(event, deliveries).catch((error) => {
        Logger.error('Failed to dispatch webhooks', error, {
          eventId: event.id,
          eventType: type,
        });
      });
    }

    return event;
  }

  private async ResolvePlatformForEvent(
    account: string,
    dataObject: EventDataObject
  ): Promise<string> {
    if ('object' in dataObject && dataObject.object === 'account') {
      const acct = dataObject as { id: string; platform_account: string };
      return acct.platform_account;
    }

    const resourceAccount = await this.accountModule.GetAccount(account);
    if (resourceAccount) {
      return GetPlatformAccountId(resourceAccount);
    }

    return account;
  }

  private async PersistDeliveries(
    event: Event,
    endpoints: WebhookEndpointRecord[]
  ): Promise<WebhookDelivery[]> {
    if (endpoints.length === 0) {
      Logger.debug('No webhook endpoints configured for event type', {
        eventId: event.id,
        eventType: event.type,
        platformAccountId: event.platform_account,
      });
      return [];
    }

    Logger.debug('Dispatching webhooks', {
      eventId: event.id,
      eventType: event.type,
      platformAccountId: event.platform_account,
      endpointCount: endpoints.length,
    });

    try {
      return await this.webhookDeliveryModule.CreateDeliveriesForEvent(
        event,
        endpoints
      );
    } catch (error) {
      Logger.error('Failed to persist webhook deliveries', error, {
        eventId: event.id,
        eventType: event.type,
      });
      return [];
    }
  }

  private async DeliverFirstAttempts(
    event: Event,
    deliveries: WebhookDelivery[]
  ): Promise<void> {
    const results = await Promise.allSettled(
      deliveries.map((delivery) =>
        this.webhookDeliveryWorker.ProcessDelivery(delivery.id)
      )
    );

    const successful = results.filter(
      (result) => result.status === 'fulfilled' && result.value === 'succeeded'
    ).length;

    Logger.info('Webhook dispatch completed', {
      eventId: event.id,
      eventType: event.type,
      platformAccountId: event.platform_account,
      successful,
      failed: results.length - successful,
      total: results.length,
    });
  }

  CreateEventObject(
    type: EventType,
    account: string,
    platformAccountId: string,
    dataObject: EventDataObject,
    options: EventOptions = {}
  ): Event {
    return this.eventModule.EventObject(
      type,
      account,
      platformAccountId,
      dataObject,
      options
    );
  }
}
