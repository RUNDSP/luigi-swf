import logging


logger = logging.getLogger(__name__)


class SwfHeartbeatCancel(object):
    """Mix-in for Luigi Tasks

    Mix-in this class to a Luigi task to make use of SWF heartbeat timeouts and
    cancellation.

    When the task is being run directly in Luigi (not SWF),
    ``self.cancel_requested`` will always be ``False``, and calling
    :meth:`heartbeat` or :meth:`ack_cancel` will have no effect. However,
    your code should not call :meth:`ack_cancel` unless
    ``self.cancel_requested == True`` anyway.
    """

    cancel_requested = False
    cancel_acked = False

    def heartbeat(self):
        """Send heartbeat to SWF and check if cancellation was requested

        If cancellation was requested, ``self.cancel_requested`` will be set
        to ``True`` after invoking this method. This method has no effect
        when the task is not being run with SWF.
        """
        if not hasattr(self, 'activity_worker'):
            logger.debug('SwfHeartbeatCancel().heartbeat(), '
                         'worker not registered')
            return
        logger.debug('SwfHeartbeatCancel().heartbeat(), %s, heartbeat',
                     self.activity_id)
        if self.activity_worker.heartbeat()['cancelRequested']:
            logger.info('SwfHeartbeatCancel().heartbeat(), %s, '
                        'cancel requested', self.activity_id)
            self.cancel_requested = True

    def ack_cancel(self):
        """Send cancellation acknowledgement to SWF"""
        if not hasattr(self, 'activity_worker'):
            logger.debug('SwfHeartbeatCancel().ack_cancel(), '
                         'worker not registered')
            return
        logger.info("SwfHeartbeatCancel().ack_cancel(), %s, ack'ing cancel",
                    self.activity_id)
        self.activity_worker.cancel()
        self.cancel_acked = True

    def register_activity_worker(self, activity_worker, activity):
        """Register the activity worker as an observer of heartbeats

        Called by :class:`luigi_swf.worker.LuigiSwfWorker` to register itself
        as the activity worker managing this instance of the task. It observes
        :meth:`heartbeat` and :meth:`ack_cancel` from this class.
        """
        self.activity_worker = activity_worker
        self.activity_id = activity['activityId']
