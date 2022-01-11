from SystemEventLogLib.Events import Event
from constants.cb_constants.system_event_log import Security


class SecurityEvents(object):
    @staticmethod
    def audit_enabled(node, enabled_audit_ids, log_path,
                      rotate_interval, rotate_size):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.AuditEnabled,
            Event.Fields.DESCRIPTION: "Audit enabled",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "new_settings": {
                    "enabled_audit_ids": enabled_audit_ids,
                    "log_path": log_path,
                    "rotate_interval": rotate_interval,
                    "rotate_size": rotate_size
                }
            }
        }

    @staticmethod
    def audit_disabled(node, disabled_users, enabled_audit_ids, log_path,
                       rotate_interval, rotate_size, sync):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.AuditDisabled,
            Event.Fields.DESCRIPTION: "Audit disabled",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "old_settings": {
                    "disabled_users": disabled_users,
                    "enabled_audit_ids": enabled_audit_ids,
                    "log_path": log_path,
                    "rotate_interval": rotate_interval,
                    "rotate_size": rotate_size,
                    "sync": sync
                }
            }
        }

    @staticmethod
    def audit_setting_changed(node, disabled_users, old_enabled_audit_ids, old_log_path,
                             old_rotate_interval, old_rotate_size, sync, new_enabled_audit_ids,
                             new_log_path, new_rotate_interval, new_rotate_size):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.AuditSettingChanged,
            Event.Fields.DESCRIPTION: "Audit configuration changed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "old_settings": {
                    "disabled_users": disabled_users,
                    "enabled_audit_ids": old_enabled_audit_ids,
                    "log_path": old_log_path,
                    "rotate_interval": old_rotate_interval,
                    "rotate_size": old_rotate_size,
                    "sync": sync
                },
                "new_settings": {
                    "enabled_audit_ids": new_enabled_audit_ids,
                    "log_path": new_log_path,
                    "rotate_interval": new_rotate_interval,
                    "rotate_size": new_rotate_size
                }
            }
        }

    @staticmethod
    def user_added(node, domain):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.UserAdded,
            Event.Fields.DESCRIPTION: "User added",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "user": "",
                "domain": domain
                }
            }

    @staticmethod
    def user_deleted(node, domain):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.UserRemoved,
            Event.Fields.DESCRIPTION: "User deleted",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "user": "",
                "domain": domain
            }
        }

    @staticmethod
    def group_added(node):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.GroupAdded,
            Event.Fields.DESCRIPTION: "Group added",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "group": "",
            }
        }

    @staticmethod
    def group_deleted(node):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.GroupRemoved,
            Event.Fields.DESCRIPTION: "Group deleted",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "group": "",
            }
        }
