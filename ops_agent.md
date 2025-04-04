# Google Ops Agent Setup (Debian Bookworm)

## 1. Download Ops Agent

```bash
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install
sudo systemctl status google-cloud-ops-agent
```

## 2. Configure log collection 

**Access the ```config.yaml``` config file**

```bash
sudo vim /etc/google-cloud-ops-agent/config.yaml
```

**Write configs**

```bash
logging:
  receivers:
    # script 1 log
    script1-logs:
      type: files
      include_paths:
        - /var/log/script1.log

    # script 2 log
    script2-logs:
      type: files
      include_paths:
        - /var/log/script2.log

  service:
    pipelines:
      default_pipeline:
        receivers: [script1-logs, script2-logs]
```

or

```bash
sudo tee /etc/google-cloud-ops-agent/config.yaml > /dev/null <<'EOF'
logging:
  receivers:
    # script 1 log
    script1-logs:
      type: files
      include_paths:
        - /var/log/script1.log

    # script 2 log
    script2-logs:
      type: files
      include_paths:
        - /var/log/script2.log

  service:
    pipelines:
      default_pipeline:
        receivers: [script1-logs, script2-logs]
EOF
```

**Apply config**

```bash
sudo systemctl restart google-cloud-ops-agent
```

## 3. Create log file and set permissions

**Create log files**

```bash
sudo touch /var/log/scrip1.log
sudo touch /var/log/scrip2.log
```

**Set permissions**

```bash
sudo chmod 777 /var/log/scrip1.logg
sudo chmod 777 /var/log/scrip2.logg
```
**Verify permissions**

```bash
ls -la /var/log/*.log
```

**Expected output**

```bash
-rwxrwxrwx 1 root google-cloud-ops-agent ... /var/log/script1.log
-rwxrwxrwx 1 root google-cloud-ops-agent ... /var/log/script2.log
```



## 4. Test log tracking

**Manually write test entries**

```bash
echo "TEST LOG ENTRY - $(date)" | sudo tee -a /var/log/scrip1.log
```

**Check local log entries**

```bash
sudo tail -f /var/log/script1.log
```

## 5. Verify logs in Google Cloud Console

**Go to** [Log Explorer](https://console.cloud.google.com/logs/query)

**Run query**

```sql
-- search by log ID
resource.type="gce_instance"
log_id("landlord_report_pipeline-logs")

-- search by log content
resource.type="gce_instance"
jsonPayload.message:"TEST LOG ENTRY"
```

**Note: it might take about 5-10 minutes for logs to appear**

## 6. Troubleshooting

**If logs do not show up**

```bash
sudo systemctl stop google-cloud-ops-agent-logging
sudo systemctl disable google-cloud-ops-agent-logging
sudo systemctl mask google-cloud-ops-agent-logging
sudo systemctl restart google-cloud-ops-agent  
```

**Verify**

```bash
sudo systemctl list-units 'google-cloud-ops-agent*'
```

Expected (or similar):

```
  UNIT                                                   LOAD   ACTIVE SUB     DESCRIPTION                           
  google-cloud-ops-agent-diagnostics.service             loaded active running Google Cloud Ops Agent - Diagnostics
  google-cloud-ops-agent-fluent-bit.service              loaded active running Google Cloud Ops Agent - Logging Agent
● google-cloud-ops-agent-logging.service                 masked failed failed  google-cloud-ops-agent-logging.service
  google-cloud-ops-agent-opentelemetry-collector.service loaded active running Google Cloud Ops Agent - Metrics Agent
  google-cloud-ops-agent.service                         loaded active exited  Google Cloud Ops Agent
```

- Modern Ops Agent (2.6.0+) versions use Fluent Bit for logging by default.
- The ```exited``` state for the main service is normal.
- Most importantly, ensure ```google-cloud-ops-agent-fluent-bit.service``` is running.
