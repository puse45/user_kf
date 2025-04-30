import json

from django.db import models

# Create your models here.


class User(models.Model):
    name = models.CharField(max_length=100)
    email = models.EmailField(unique=True)
    address = models.TextField()
    phone = models.CharField(max_length=20)

    def __str__(self):
        return self.email


class FailedEvent(models.Model):
    topic = models.CharField(max_length=100)
    event_data = models.JSONField()
    error_message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    processed = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.topic} at {self.created_at}"

    def retry_event(self):
        from .kafka_producer import producer
        try:
            producer.produce(
                self.topic,
                json.dumps(self.event_data)
            )
            producer.flush()
            self.processed = True
            self.save()
            return True
        except Exception as e:
            self.error_message = str(e)
            self.save()
            return False
