import logging


logger = logging.getLogger(__name__)


class SwfHeartbeatCancel(object):
    """Mix-in for Luigi Tasks"""

    cancel_requested = False

    def heartbeat(self):
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
        if not hasattr(self, 'activity_worker'):
            logger.debug('SwfHeartbeatCancel().ack_cancel(), '
                         'worker not registered')
            return
        logger.info("SwfHeartbeatCancel().ack_cancel(), %s, ack'ing cancel",
                    self.activity_id)
        self.activity_worker.cancel()

    def register_activity_worker(self, activity_worker, activity_id):
        self.activity_worker = activity_worker
        self.activity_id = activity_id
