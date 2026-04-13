# Vulture whitelist for TAF
# Add false positives here that vulture incorrectly flags as dead code

# Framework entry points (called dynamically)
testrunner.main
setUp
tearDown
setUpClass
tearDownClass

# SDK client methods (called via reflection)
SDKClient.__init__
SDKClient.connect
SDKClient.disconnect

# Test input methods (called via TestInputSingleton)
TestInput.parse_params

# REST API methods (called dynamically)
RestConnection.get_pools_default
RestConnection.get_nodes
RestConnection.create_bucket

# Decorator-registered functions
task.async_task
