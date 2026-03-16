# TAF Troubleshooting Guide

## Common Setup Issues

### Submodule Initialization Failure
**Problem**: `fatal: Not a git repository` when running `git submodule`
**Cause**: Submodule paths not initialized
**Solution**:
```bash
git submodule init
git submodule update --init --force --remote
```

### Python Version Incompatibility
**Problem**: `ModuleNotFoundError` or dependency import errors
**Cause**: Python version mismatch (requires 3.10+)
**Solution**:
```bash
python --version  # Should be 3.10+
pyenv install 3.10.14
pyenv local 3.10.14
python -m pip install -r requirements.txt
```

### Missing Dependencies
**Problem**: `ImportError: No module named 'couchbase'`
**Cause**: Requirements not installed
**Solution**:
```bash
python -m pip install -r requirements.txt
```

### Cluster Connection Failure
**Problem**: `Connection refused` or timeout errors
**Cause**: Cluster not running or wrong IP/credentials
**Solution**:
- Verify cluster is running: `curl http://<ip>:8091/pools`
- Check node.ini for correct IPs and credentials
- Ensure SSH access works: `ssh root@<ip>`

## Common Test Failures

### Test Not Found
**Problem**: `ImportError: No module named 'epengine.basic_ops.basic_ops'`
**Cause**: Incorrect test module path
**Solution**:
- Verify module exists in `pytests/`
- Check import path matches directory structure
- Ensure proper PYTHONPATH set in testrunner.py

### Missing Test Parameters
**Problem**: Tests fail with missing required parameters
**Cause**: Parameter not passed via command line or .conf
**Solution**:
- Add parameter to command line: `-p get-cbcollect-info=True`
- Or include in .conf file: `test_name,param1=value1`
- Check test code for `TestInputSingleton.input.param()` calls

### Cluster Reset Failures
**Problem**: "Cluster reset failed" or "Bucket deletion timeout"
**Cause**: Cluster in bad state or insufficient cleanup
**Solution**:
- Use `-p skip_cluster_reset=True` to preserve cluster state
- Manually clean cluster via REST API or CLI
- Check for stuck services or rebalance in progress

### Document Loading Issues
**Problem**: Tests timeout during document load
**Cause**: Insufficient capacity or DocLoader issues
**Solution**:
- Reduce num_items parameter
- Check DocLoader subprocess status
- Verify cluster resources (memory, CPU)
- Use Java loader: `--launch_java_doc_loader`

### SDK Connection Errors
**Problem**: `LCB_ERR_TIMEOUT` or connection failures
**Cause**: Network issues or cluster overload
**Solution**:
- Check cluster logs for errors
- Verify network connectivity
- Reduce concurrent operations
- Check SDK version compatibility

## Log Locations

### Test Execution Logs
- **Location**: `logs/testrunner-<timestamp>/`
- **Pattern**: `logs/testrunner-yy-mmm-dd_HH-MM-SS/`
- **Contents**: Individual test logs, framework logs

### Couchbase Server Logs
- **Linux**: `/opt/couchbase/var/log/couchbase/`
- **Key files**:
  - `info.log` – General server logs
  - `error.log` – Error messages
  - `debug.log` – Debug information
  - `stats.log` – Performance metrics

### CBCollect (Server Diagnostics)
- **Automatic**: Collected on test failure if `get-cbcollect-info=True`
- **Manual**: `cbcollect <cluster> > cbcollect.zip`
- **Location**: Test output directory or archives folder

### Application Logs
- **TAF logs**: `logs/testrunner-<timestamp>/testrunner.log`
- **DocLoader logs**: `logs/testrunner-<timestamp>/doc_loader/`
- **Sirius logs**: `logs/testrunner-<timestamp>/sirius/`

## Retry Strategies

### Individual Test Retry
```bash
# Rerun with same parameters
python testrunner.py -i node.ini -t <failing_test,param=value>
```

### Test Suite Retry
```bash
# Rerun entire suite
python testrunner.py -i node.ini -c conf/collections/collections_rebalance.conf -r
```

### Exclude Failing Tests
```bash
# Skip problematic tests temporarily
python testrunner.py -i node.ini -c conf/sanity.conf -e "upgrading.*,volatile_tests.*"
```

### Rerun from Checkpoint
```bash
# Use skip_cluster_reset to preserve state
python testrunner.py -i node.ini -c conf/collections/collections_rebalance.conf -p skip_cluster_reset=True
```

## Common Network Issues

### SSH Connection Refused
**Problem**: Cannot SSH to cluster nodes
**Cause**: SSH service not running or wrong credentials
**Solution**:
- Verify SSH access: `ssh -v root@<ip>`
- Check /etc/ssh/sshd_config for PermitRootLogin
- Restart SSHD: `systemctl restart sshd`

### Firewall Blocking
**Problem**: Connection timeouts or refused connections
**Cause**: Firewall rules blocking Couchbase ports
**Solution**:
- Allow ports: 8091 (REST), 8092-8096 (services), 9102 (index), 8093 (query)
- Test connectivity: `telnet <ip> 8091`
- Check firewall rules: `iptables -L` or `firewall-cmd --list-all`

### DNS Resolution Failure
**Problem**: Name resolution errors
**Cause**: DNS misconfiguration or incorrect hostnames
**Solution**:
- Use IP addresses instead of hostnames in node.ini
- Check /etc/hosts for mappings
- Test DNS: `nslookup <hostname>`

## Escalation Points

### Data Loss or Corruption
1. Stop all test execution immediately
2 Collect cbcollect from all nodes
3 Check server logs for storage errors
4 Contact Couchbase support with logs
5 Do not attempt recovery without guidance

### Continuous Test Failures
1. Check cluster health: `curl http://<ip>:8091/pools/default`
2 Verify server version compatibility
3 Review recent code changes
4 Run basic sanity tests to isolate issues
5 Compare with known-good test runs

### Platform-Specific Issues
**On-Premise**:
- Check OS logs: `/var/log/messages` or `journalctl -xe`
- Verify system resources: `top`, `free -m`, `df -h`
- Check Couchbase process: `ps aux | grep couchbase`

**Capella**:
- Check Capella console for cluster status
- Verify network security groups/firewall rules
- Review Capella audit logs
- Contact Capella support for cluster issues

```couchbase-utils/cb_server_rest_util/cluster_nodes/cluster_init_provision.py`**
- Verify services are enabled on correct nodes
- Check cluster configuration JSON for errors
- Validate bucket and scope creation parameters

## Performance Issues

### Slow Test Execution
**Problem**: Tests taking longer than expected
**Cause**: Insufficient cluster resources or network latency
**Solution**:
- Check cluster statistics for bottlenecks
- Reduce concurrent operations
- Verify no network throttling
- Check if cluster is overloaded with other workloads

### Memory Pressure
**Problem**: DGM state or eviction errors
**Cause**: Insufficient RAM for data set
**Solution**:
- Reduce num_items or document size
- Increase cluster memory quota
- Use smaller document templates
- Check bucket eviction policies

### CPU Saturation
**Problem**: CPU utilization near 100%
**Cause**: Insufficient compute resources
**Solution**:
- Scale cluster to larger instance types
- Reduce load intensity
- Check for background operations
- Verify no runaway processes

## Debugging Techniques

### Enable Verbose Logging
```bash
python testrunner.py -i node.ini -c conf/sanity.conf -l DEBUG
```

### Test Discovery Only
```bash
python testrunner.py -i node.ini -c conf/sanity.conf -n
```

### Examine Test Parameters
```python
# In test code, print all parameters
print(TestInputSingleton.input.test_params)
```

### Interactive Debugging
```bash
# Install ipdb for IPython debugging
python -m pip install ipdb

# Use in test code with breakpoints
import ipdb; ipdb.set_trace()
```

### Monitor Cluster During Tests
```bash
# Watch cluster stats in real-time
watch -n 1 'curl -s http://<ip>:8091/pools/default/buckets | jq'

# Monitor rebalance progress
curl http://<ip>:8091/pools/default/rebalanceProgress
```

## Known Issues and Workarounds

### Submodule Sync Issues
**Problem**: DocLoader submodule not updating
**Workaround**: Manually update submodule path
```bash
cd DocLoader
git pull origin main
cd ..
```

### Python 3.12 Compatibility
**Problem**: Some dependencies not compatible with Python 3.12
**Workaround**: Use Python 3.10.14 as specified in README

### Large File Git Operations
**Problem**: Large log files or git history causing slow operations
**Workaround**: Exclude large files from git in .gitignore

### Cluster Initialization Timeout
**Problem»: Cluster init takes too long
**Workaround**: Increase timeout in test parameters or check network connectivity
