from django.contrib import admin

from users.models import User, FailedEvent


# Register your models here.

@admin.register(User)
class UserAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'email','address', 'phone')
    search_fields = ('id', 'name')


@admin.register(FailedEvent)
class FailedEventAdmin(admin.ModelAdmin):
    list_display = ('id', 'topic', 'created_at','processed',)
    search_fields = ('id', 'topic')
    list_filter = ('created_at',)

