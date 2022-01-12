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
    def audit_disabled(node, enabled_audit_ids, log_path,
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
                    "enabled_audit_ids": enabled_audit_ids,
                    "log_path": log_path,
                    "rotate_interval": rotate_interval,
                    "rotate_size": rotate_size,
                    "sync": sync
                }
            }
        }

    @staticmethod
    def audit_setting_changed(node, old_enabled_audit_ids, old_log_path,
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

    @staticmethod
    def password_policy_changed(node, old_min_length, old_must_present,
                                new_min_length, new_must_present):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.PasswordPolicyChanged,
            Event.Fields.DESCRIPTION: "Password policy changed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "old_settings": {
                    "min_length": old_min_length,
                    "must_present": old_must_present
                },
                "new_settings": {
                    "min_length": new_min_length,
                    "must_present": new_must_present
                },
            }
        }

    @staticmethod
    def sasldauth_config_changed(node, old_enabled, old_admins, old_roAdmins,
                                  new_enabled, new_admins, new_roAdmins):
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.SasldAuthConfigChanged,
            Event.Fields.DESCRIPTION: "sasldauth config changed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "old_settings": {
                    "enabled": old_enabled,
                    "admins": old_admins,
                    "roAdmins": old_roAdmins
                },
                "new_settings": {
                    "enabled": new_enabled,
                    "admins": new_admins,
                    "roAdmins": new_roAdmins
                },
            }
        }

    @staticmethod
    def ldap_config_changed(node, old_settings_map, new_settings_map):
        """
        Note: Since the settings list is huge, the parameters accepted are in
        dictionary format as against to individual keys
        """
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.LdapConfigChanged,
            Event.Fields.DESCRIPTION: "LDAP configuration changed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "old_settings": old_settings_map,
                "new_settings": new_settings_map
            }
        }

    @staticmethod
    def security_config_changed(node, old_settings_map, new_settings_map):
        """
        Note: Since the settings list is huge, the parameters accepted are in
        dictionary format as against to individual keys
        """
        return {
            Event.Fields.COMPONENT: Event.Component.SECURITY,
            Event.Fields.SEVERITY: Event.Severity.INFO,
            Event.Fields.EVENT_ID: Security.SecurityConfigChanged,
            Event.Fields.DESCRIPTION: "Security config changed",
            Event.Fields.NODE_NAME: node,
            Event.Fields.OTP_NODE: "ns_1@" + str(node),
            Event.Fields.EXTRA_ATTRS: {
                "old_settings": old_settings_map,
                "new_settings": new_settings_map
            }
        }
