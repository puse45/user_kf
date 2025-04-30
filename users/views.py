from django.shortcuts import render
from rest_framework import viewsets
from rest_framework.response import Response

# Create your views here.
from .models import User
from .serializers import UserSerializer
from .kafka_producer import send_user_update_event

from rest_framework.decorators import api_view
from confluent_kafka import Producer


@api_view(['GET'])
def health_check(request):
    status = {
        'database': check_database(),
        'kafka': check_kafka(),
        'status': 'healthy'
    }

    if not all(status.values()):
        status['status'] = 'degraded'

    return Response(status)


def check_database():
    from django.db import connection
    try:
        connection.ensure_connection()
        return True
    except Exception:
        return False


def check_kafka():
    try:
        test_producer = Producer({'bootstrap.servers': 'localhost:9092'})
        test_producer.list_topics(timeout=5)
        return True
    except Exception:
        return False

class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer

    def perform_update(self, serializer):
        instance = serializer.save()
        send_user_update_event(instance)