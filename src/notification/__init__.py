"""Notification package — Discord / LINE / X への自動投稿。"""
from .dispatcher import NotificationDispatcher, NotifyLevel

__all__ = ["NotificationDispatcher", "NotifyLevel"]
