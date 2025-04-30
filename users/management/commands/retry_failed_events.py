from django.core.management.base import BaseCommand
from users.models import FailedEvent
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Retry failed Kafka events from database'

    def handle(self, *args, **options):
        failed_events = FailedEvent.objects.filter(processed=False)
        for event in failed_events:
            if event.retry_event():
                self.stdout.write(f"Successfully retried event {event.id}")
            else:
                self.stdout.write(f"Failed to retry event {event.id}")