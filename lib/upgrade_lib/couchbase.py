from testconstants import CB_RELEASE_BUILDS

min_compatible_version = "7.1"
features = {
    "6.5": ["durability"],
    "7.0": ["collections"],
    "7.1": ["magma", "system_event_logs"],
    "7.2": ["cdc"],
    "8.0": ["durability_impossible_fallback"],
}

upgrade_chains = {
    "7.1.0": ["7.1.0-" + CB_RELEASE_BUILDS["7.1.0"]],
    "7.1.1": ["7.1.1-" + CB_RELEASE_BUILDS["7.1.1"]],
    "7.1.2": ["7.1.2-" + CB_RELEASE_BUILDS["7.1.2"]],
    "7.1.3": ["7.1.3-" + CB_RELEASE_BUILDS["7.1.3"]],
    "7.1.4": ["7.1.4-" + CB_RELEASE_BUILDS["7.1.4"]],
    "7.1.5": ["7.1.5-" + CB_RELEASE_BUILDS["7.1.5"]],
    "7.1.6": ["7.1.6-" + CB_RELEASE_BUILDS["7.1.6"]],

    "7.2.0": ["7.2.0-" + CB_RELEASE_BUILDS["7.2.0"]],
    "7.2.1": ["7.2.1-" + CB_RELEASE_BUILDS["7.2.1"]],
    "7.2.2": ["7.2.2-" + CB_RELEASE_BUILDS["7.2.2"]],
    "7.2.3": ["7.2.3-" + CB_RELEASE_BUILDS["7.2.3"]],
    "7.2.4": ["7.2.4-" + CB_RELEASE_BUILDS["7.2.4"]],
    "7.2.5": ["7.2.5-" + CB_RELEASE_BUILDS["7.2.5"]],
    "7.2.6": ["7.2.6-" + CB_RELEASE_BUILDS["7.2.6"]],
    "7.2.7": ["7.2.7-" + CB_RELEASE_BUILDS["7.2.7"]],
    "7.2.8": ["7.2.8-" + CB_RELEASE_BUILDS["7.2.8"]],

    "7.6.0": ["7.6.0-" + CB_RELEASE_BUILDS["7.6.0"]],
    "7.6.1": ["7.6.0-" + CB_RELEASE_BUILDS["7.6.1"]],
    "7.6.2": ["7.6.0-" + CB_RELEASE_BUILDS["7.6.2"]],
    "7.6.3": ["7.6.0-" + CB_RELEASE_BUILDS["7.6.3"]],

    # Ref: https://docs.couchbase.com/server/current/install/upgrade.html
    "6.6.4_7.2.3": ["6.6.4-" + CB_RELEASE_BUILDS["6.6.4"],
                    "7.2.3-" + CB_RELEASE_BUILDS["7.2.3"]],
    "6.6.5_7.2.3": ["6.6.5-" + CB_RELEASE_BUILDS["6.6.5"],
                    "7.2.3-" + CB_RELEASE_BUILDS["7.2.3"]],
    "6.6.5_7.2.0": ["6.6.5-" + CB_RELEASE_BUILDS["6.6.5"],
                    "7.2.0-" + CB_RELEASE_BUILDS["7.2.0"]],

    "7.0.0_7.1.0": ["7.0.0-" + CB_RELEASE_BUILDS["7.0.0"],
                    "7.1.0-" + CB_RELEASE_BUILDS["7.1.0"]],
    "7.0.0_7.1.5": ["7.0.0-" + CB_RELEASE_BUILDS["7.0.0"],
                    "7.1.5-" + CB_RELEASE_BUILDS["7.1.5"]],
}
