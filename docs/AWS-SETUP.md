# AWS setup — click by click

A complete walkthrough for deploying this repo to AWS, assuming you have **never
used AWS before**. Every console action is spelled out: where to click, what to
type, and what you should see afterwards.

**Read this line before anything else:** do the steps in order, and do not skip a
"✓ Check" box. Almost every way this deployment goes wrong is caught by one of
them, and finding out four steps later is what turns an afternoon into a weekend.

**What you end up with:** two Linux servers, a managed database, and two AWS
networking pieces in front. Roughly **$40–52/month**, so about four months on the
$200 credit. [COSTS.md](COSTS.md) has the breakdown.

```
customer
   │ HTTPS  api.yourdomain.com
   ▼
CloudFront ──▶ ALB ──▶ box-edge   t4g.small
                       ├─ edge      Fastify :3000
                       └─ dispatch  systemd timers (1 min + hourly)
                            │ produce · private IP :9092
                            ▼
                       box-kafka  t4g.small   kafka.yourdomain.com
                       └─ Kafka   :9092 internal (VPC only)
                                  :9094 public SASL_SSL
                            │
                   ClickHouse Cloud pulls :9094
                            │  signal_log
                            ▼
        dispatcher reads the archive ──▶ payments /api/internal/settle
```

### Your scratch sheet

Open a text file now and keep it beside you. You will fill these in as you go, and
later steps need them. Do not rely on remembering.

```
REGION                  = ..................  (Step 1)
KEYPAIR FILE            = ~/.ssh/signal-prod.pem
CLICKHOUSE URL          = https://..........clickhouse.cloud:8443
CLICKHOUSE PASSWORD     = ..................
CLICKHOUSE EGRESS IPS   = .................. (6 addresses, Step 2)
VPC ID                  = vpc-..............  (Step 4)
SUBNET A  (both boxes)  = subnet-...........  AZ ......
SUBNET B  (ALB only)    = subnet-...........  AZ ......
SG-ALB ID               = sg-...............  (Step 5)
SG-EDGE ID              = sg-...............
SG-KAFKA ID             = sg-...............
ELASTIC IP              = ..................  (Step 6)
BOX-KAFKA PRIVATE IP    = 10.................  (Step 7)
KAFKA SCRAM PASSWORD    = ..................  (Step 10)
ALB DNS NAME            = ..........elb.amazonaws.com  (Step 14)
CLOUDFRONT DOMAIN       = d.........cloudfront.net     (Step 15)
INTERNAL_SETTLE_SECRET  = ..................
```

---

## Step 0 — The words AWS uses

You do not need to understand AWS to follow this. You do need these nine words,
because the console assumes them. Read once, then move on.

| Word | What it actually is | Analogy |
| --- | --- | --- |
| **Region** | a physical group of datacentres, e.g. `ap-south-1` = Mumbai | which city your stuff lives in |
| **Availability Zone (AZ)** | one isolated datacentre inside a region, e.g. `ap-south-1a` | which building in that city |
| **VPC** | your own private network inside AWS | the office building's internal wiring |
| **Subnet** | a slice of that network, living in exactly one AZ | one floor of the building |
| **Security group (SG)** | a firewall attached to a server — a list of "who may connect, on which port" | the door policy |
| **EC2 instance** | a virtual Linux server you SSH into | a rented computer |
| **Elastic IP (EIP)** | a public IP address that does not change | a permanent street address |
| **ALB** | load balancer: takes HTTPS, forwards to your server, health-checks it | the receptionist |
| **CloudFront** | AWS's CDN — the public front door, close to your users | the front gate |

Two facts about regions that cause 90 % of beginner confusion:

- **Everything belongs to one region.** A server in Mumbai cannot see a security
  group in Virginia.
- **The region selector is the dropdown at the top-right of the console.** If a
  list looks empty and you're sure you created something, you are in the wrong
  region. Check that dropdown *first*, every time.

**About VPCs, since it is the question everyone asks:** yes, this needs a VPC — and
your account already has one. AWS creates a **default VPC** in every region, with a
subnet in every AZ and internet access already wired up. You will *use* it, not
build it. Step 4 is just confirming it's there and writing down two subnet IDs.
Building a custom VPC would add eight steps and change nothing at this scale.

---

## Step 1 — AWS account setup

### 1.1 Pick your region and write it down

1. Sign in at **console.aws.amazon.com**.
2. Top-right, click the **region dropdown** (it will say something like
   *N. Virginia*).
3. Choose one region and use it for the entire rest of this document:
   - **`ap-south-1` (Mumbai)** if you are in India — ~17 % cheaper and lower latency.
   - **`us-east-1` (N. Virginia)** otherwise.
4. Write it on your scratch sheet as `REGION`.

> From here on, **every** console screenshot in your browser must show this region
> in the top-right. If it doesn't, fix it before clicking anything.

### 1.2 Set a budget alert — before spending anything

A $200 credit with no alarm is how people wake up to a bill.

1. Click your **account name** (top-right) → **Billing and Cost Management**.
2. Left sidebar → **Budgets**.
3. **Create budget**.
4. Choose **Use a template (simplified)** → **Monthly cost budget**.
5. **Budget name**: `signal-pipeline`
6. **Enter your budgeted amount**: `50`
7. **Email recipients**: your email.
8. **Create budget**.

**✓ Check:** the Budgets list shows `signal-pipeline` with a $50 monthly amount.

### 1.3 Create the SSH keypair

This is the file that lets you log into your servers. **AWS shows you the private
key exactly once.** Lose it and you cannot get into a running server — only replace
it.

1. In the search bar at the top, type **EC2** and click the **EC2** service.
2. Left sidebar → under *Network & Security* → **Key pairs**.
3. **Create key pair** (orange button, top-right).
4. **Name**: `signal-prod`
5. **Key pair type**: **RSA**
6. **Private key file format**: **.pem**
7. **Create key pair** → your browser downloads `signal-prod.pem`.

Then, in your own terminal:

```bash
mkdir -p ~/.ssh
mv ~/Downloads/signal-prod.pem ~/.ssh/
chmod 400 ~/.ssh/signal-prod.pem
```

`chmod 400` means "only I can read this". **SSH refuses to use a key that other
users could read**, with an error about *unprotected private key file*. This one
command prevents that.

**✓ Check:** `ls -l ~/.ssh/signal-prod.pem` shows `-r--------`.

### 1.4 Find your own public IP

```bash
curl ifconfig.me
```

Write it down. Step 5 uses it so that only *you* can SSH to the servers.

> Home internet connections change this occasionally. If SSH suddenly times out in
> a few weeks, re-run this command and update the security group rules from Step 5.

---

## Step 2 — ClickHouse Cloud

ClickHouse is the database that stores every signal. It is managed by ClickHouse
(not AWS), and it **pulls from Kafka by itself** — which is why there is no
consumer program for you to deploy.

### 2.1 Create the service

1. Sign up at **clickhouse.cloud**.
2. **Create new service**.
3. **Cloud provider**: AWS. **Region**: the same one as your `REGION`, or the
   nearest available.
4. Create it. Wait for the status to become **Running** (a minute or two).
5. On the service page, click **Connect**. Copy:
   - the **HTTPS endpoint** — looks like `https://xxxxx.region.aws.clickhouse.cloud:8443`
   - the **password** for the `default` user (shown at creation; resettable later)
6. Put both on your scratch sheet.

**✓ Check:** you can open the service's **SQL console** from the left sidebar and
run `SELECT 1`.

### 2.2 Get the egress IP list — the step people skip

The **egress IPs** are the addresses ClickHouse Cloud connects *out* from. Step 5
uses them so that *only* ClickHouse can reach your Kafka port, and nobody else.

These are **not** on the service settings page and **not** on a documentation page,
which is why they look discontinued. They come from a public JSON API. Verified
live 2026-08-27:

```bash
# everything, all clouds and regions — to browse
curl -s https://api.clickhouse.cloud/static-ips.json | jq .

# just yours — replace ap-south-1 with YOUR region
curl -s https://api.clickhouse.cloud/static-ips.json \
  | jq -r '.aws[] | select(.region=="ap-south-1") | .egress_ips[]'
```

You get about six addresses. Write all of them down.

**Take `egress_ips`, NOT `clickpipes_egress_ips`.** Every region lists both, and
they are different addresses:

| Field | Used by |
| --- | --- |
| `egress_ips` | native engines — the **Kafka table engine**, which is what this build uses |
| `clickpipes_egress_ips` | ClickPipes only, a separate product not used here |

That split is why searching for this online turns up ClickPipes material and makes
ClickPipes look mandatory. **It is not.** The Kafka table engine has its own
documented egress addresses, and they are what make Step 5's locked-down firewall
possible.

> **These are not guaranteed permanent.** If ingestion silently stops months from
> now — Step 12's check showing a live consumer, connect exceptions, and
> `num_messages_read` stuck — re-fetch this JSON and compare it against your
> security group *before* debugging anything else.

**✓ Check:** you have ~6 IP addresses written down.

---

## Step 3 — GoDaddy DNS orientation

You will create DNS records in Steps 9, 14 and 15. Right now, just learn the panel
and one gotcha.

**Where:** GoDaddy → sign in → **Domain Portfolio** → click your domain → **DNS**
(or *Manage DNS*) → **Add New Record**.

### The gotcha that will bite you

GoDaddy's **Name** field is **relative to your domain** — it appends the domain
for you.

| You want | Type in Name | NOT |
| --- | --- | --- |
| `kafka.yourdomain.com` | `kafka` | ~~`kafka.yourdomain.com`~~ |
| `api.yourdomain.com` | `api` | ~~`api.yourdomain.com`~~ |
| `yourdomain.com` itself | `@` | ~~blank~~ |

Typing the full name creates `kafka.yourdomain.com.yourdomain.com`, which resolves
to nothing and produces **no error message at all**. When a `dig` comes back empty,
check this first.

**Set TTL to 600 seconds (10 minutes)** on every record while building. TTL is how
long the world caches an answer; GoDaddy defaults to 1 hour, which turns each typo
into an hour of waiting.

### You do not need Route53

Keeping DNS at GoDaddy works for everything here. (Route53 has "alias" records
GoDaddy lacks; this build doesn't need them — see Step 15 for why `api` is a CNAME.)

---

## Step 4 — Confirm your VPC and pick subnets

You are not creating a network. You are looking at the one AWS already made, and
writing down three IDs.

1. Top search bar → type **VPC** → click the **VPC** service.
2. Left sidebar → **Your VPCs**.
3. You should see one row. Look at the **Default VPC** column — it says **Yes**.
4. Copy its **VPC ID** (`vpc-0abc123...`) to your scratch sheet.

> **If the list is empty**, your account's default VPC was deleted at some point.
> Fix: **Actions** (top-right) → **Create default VPC** → **Create**. Takes a few
> seconds and gives you exactly what a fresh account has.

Now the subnets:

5. Left sidebar → **Subnets**.
6. Filter to your VPC if there are many (search box → paste your VPC ID).
7. You will see one subnet per Availability Zone — typically 3 or 4 rows.
8. Pick **two** of them, in **different** AZs. Write down both **Subnet ID**s and
   their **Availability Zone**s as `SUBNET A` and `SUBNET B`.

**What each is for:**

- **`SUBNET A`** — both EC2 servers go here. **Both in the same subnet**, which
  means the same AZ, which means the traffic between them (edge → Kafka, thousands
  of messages) is free. Putting them in different AZs works but bills you per GB
  for that traffic, forever, for no benefit.
- **`SUBNET B`** — used only in Step 14. An ALB is *required* to span at least two
  AZs, so it needs a second subnet even though only one server exists. This costs
  nothing.

**✓ Check:** your scratch sheet has one VPC ID and two subnet IDs with two
different AZ names.

<details>
<summary><b>Worked example — what this looks like in a real account</b></summary>

A default VPC in `us-east-1` looks like this. Yours will have different IDs but the
same shape:

```
VPC ID                  = vpc-0078a3f80019a9b13     172.31.0.0/16   (default)

SUBNET A  (both boxes)  = subnet-0f334450a4344f3b3  us-east-1a  172.31.0.0/20
SUBNET B  (ALB only)    = subnet-013e8be20f57a9fee  us-east-1b  172.31.80.0/20
                          subnet-09661b6f7d58aeaa4  us-east-1d  172.31.32.0/20
                          subnet-06a65520908ee4af6  us-east-1f  172.31.64.0/20
```

`172.31.0.0/16` is the standard default-VPC range, and each subnet is a `/20` with
~4,091 usable addresses — far more than this build needs.

**Which two you pick does not matter**, as long as they are in different AZs. If a
launch later fails with *InsufficientInstanceCapacity*, choose a different subnet
from the list and retry.
</details>

> **Are these subnets public?** In a default VPC, yes — every subnet has a route to
> the internet gateway. That matters because your servers need to download packages
> and reach ClickHouse Cloud. You do not have to configure anything for this.

---

## Step 5 — Three security groups (the firewalls)

A security group is a list of "who may connect, on which port". You need three,
because the three roles have different rules.

**Create all three empty first, then add rules.** The rules refer to each other by
name, so a rule you'd want to write now would reference a group that doesn't exist
yet.

### 5.1 Create the three, empty

For **each** of the three names below:

1. **EC2** service → left sidebar, under *Network & Security* → **Security Groups**.
2. **Create security group**.
3. **Security group name**: as per the table.
4. **Description**: as per the table (AWS requires one).
5. **VPC**: select your `VPC ID` — **not** any other option in the dropdown.
6. Leave inbound and outbound rules **untouched** for now.
7. **Create security group**.
8. Copy the resulting **Security group ID** (`sg-0abc...`) to your scratch sheet.

| Name | Description |
| --- | --- |
| `sg-alb` | load balancer |
| `sg-edge` | edge server |
| `sg-kafka` | kafka server |

**✓ Check:** three groups exist, all showing your VPC ID, all with 0 inbound rules.

### 5.2 Add rules to `sg-alb`

1. **Security Groups** → click **`sg-alb`**.
2. Tab **Inbound rules** → **Edit inbound rules** → **Add rule**.

| Type | Port | Source | Description |
| --- | --- | --- | --- |
| HTTPS | 443 | **Anywhere-IPv4** (`0.0.0.0/0`) | public HTTPS |

3. **Save rules**.

This is intentionally open — it is the public front door. Step 17 tightens it so
only CloudFront can use it.

### 5.3 Add rules to `sg-edge`

1. Click **`sg-edge`** → **Edit inbound rules** → add two rules:

| Type | Port | Source | How to select it |
| --- | --- | --- | --- |
| Custom TCP | **3000** | **`sg-alb`** | change Source type to **Custom**, then type `sg-` in the box and pick `sg-alb` |
| SSH | 22 | **My IP** | Source type **My IP** fills your address automatically |

2. **Save rules**.

> **Rule 1 is the one people get wrong.** The source is the *security group*
> `sg-alb`, **not** an IP address and not `0.0.0.0/0`. It means "any server wearing
> the sg-alb badge may connect" — which is how the load balancer gets in while the
> internet stays out. Getting this wrong produces a target group stuck at
> **unhealthy** in Step 14 with no useful error message.

### 5.4 Add rules to `sg-kafka`

1. Click **`sg-kafka`** → **Edit inbound rules**.
2. Add these:

| Type | Port | Source | Note |
| --- | --- | --- | --- |
| Custom TCP | **9092** | **`sg-edge`** | the edge produces signals here, over the private network |
| Custom TCP | **9094** | ClickHouse egress IP #1 `/32` | **one rule per IP** |
| Custom TCP | **9094** | ClickHouse egress IP #2 `/32` | |
| Custom TCP | **9094** | ...and so on for all ~6 | |
| SSH | 22 | **My IP** | |
| HTTP | 80 | **Anywhere-IPv4** | **temporary** — for the certificate in Step 10. You will remove it in 10.8. |

3. **Save rules**.

For each ClickHouse IP: Source type **Custom**, and type the address followed by
`/32` — e.g. `13.200.35.43/32`. `/32` means "exactly this one address".

> **Never set port 9094 to `0.0.0.0/0`.** The listener uses TLS + password auth, so
> an open port would still be encrypted and authenticated rather than an outright
> leak — but it would put Kafka's pre-authentication code and your credentials in
> front of the entire internet for no benefit, when six precise rules do the job.
> If you cannot get the egress list, stop and sort that out rather than opening the
> port.

**✓ Check:** `sg-kafka` shows ~9 inbound rules; `sg-edge` shows 2; `sg-alb` shows 1.

---

## Step 6 — One Elastic IP

An Elastic IP is a public address that does not change. `box-kafka` needs one
because two things get permanently pinned to it: a DNS record and a TLS
certificate. A default EC2 public address changes every time the server stops and
starts, which would silently break both.

1. **EC2** → left sidebar, *Network & Security* → **Elastic IPs**.
2. **Allocate Elastic IP address** → leave defaults → **Allocate**.
3. Copy the address to your scratch sheet as `ELASTIC IP`.

`box-edge` gets no Elastic IP — the load balancer finds it by private address and
nothing points DNS at it.

> An Elastic IP is **free while attached to a running server** and **billed when
> left unattached**. If you tear this deployment down, release it.

**✓ Check:** one Elastic IP listed, *Associated instance* empty for now.

---

## Step 7 — Launch box-kafka

1. **EC2** → left sidebar → **Instances** → **Launch instances**.
2. **Name**: `box-kafka`
3. **Application and OS Images**:
   - Select **Ubuntu**.
   - In the AMI dropdown pick the current **Ubuntu Server LTS** (**26.04 LTS**;
     24.04 also works — the commands below are the same either way).
   - **Architecture dropdown: change it to `64-bit (Arm)`.** This is easy to miss
     and it is mandatory — the instance type below is ARM, and an x86 image will
     not boot on it.
4. **Instance type**: `t4g.small`
   *(If `t4g` types don't appear, you skipped the Arm architecture in step 3.)*
5. **Key pair (login)**: `signal-prod`
6. **Network settings** → click **Edit**:
   - **VPC**: your `VPC ID`
   - **Subnet**: your `SUBNET A` — **pick it explicitly.** The dropdown defaults to
     **`No preference`**, which lets AWS choose the AZ for you. See the warning
     below.
   - **Auto-assign public IP**: **Enable** (default-VPC subnets usually already do)
   - **Firewall (security groups)**: choose **Select existing security group**,
     then pick **`sg-kafka`** — and only that one.

> **Never leave Subnet on `No preference`.** AWS picks an AZ, and if this instance
> and `box-edge` end up in *different* AZs then every message the edge sends to
> Kafka — thousands per minute — becomes cross-AZ traffic billed per GB, forever,
> for no benefit. Both instances must name the same subnet.
7. **Configure storage**: `30` GiB, `gp3`.
   *(Kafka keeps 7 days of messages on this disk; 30 GiB is generous at your volume.)*
8. **Launch instance**.

Then attach the Elastic IP:

9. **Elastic IPs** → select yours → **Actions** → **Associate Elastic IP address**.
10. **Resource type**: Instance. **Instance**: `box-kafka`. **Associate**.

Then collect the private address:

11. **Instances** → click `box-kafka` → the **Details** tab.
12. Find **Private IPv4 address** — something like `10.0.1.23`. Copy it to your
    scratch sheet as `BOX-KAFKA PRIVATE IP`.

> This private address is what `box-edge` will use to reach Kafka, and it goes into
> two config files. It is stable across stop/start — only terminating the instance
> changes it.

**✓ Check:**

```bash
ssh -i ~/.ssh/signal-prod.pem ubuntu@<ELASTIC IP>
```

You get an Ubuntu shell. If it hangs, `sg-kafka`'s SSH rule doesn't have your
current IP. If it says *permission denied (publickey)*, the username is `ubuntu`,
not `root` or your own name.

---

## Step 8 — Launch box-edge

Identical to Step 7 with three differences, and no Elastic IP.

1. **Instances** → **Launch instances**.
2. **Name**: `box-edge`
3. **Ubuntu Server LTS** (same AMI as box-kafka), architecture **`64-bit (Arm)`**.
4. **Instance type**: `t4g.small`
5. **Key pair**: `signal-prod`
6. **Network settings** → **Edit**:
   - **VPC**: your `VPC ID`
   - **Subnet**: **`SUBNET A`** — pick it explicitly, the *same* subnet as
     box-kafka. Not `No preference`; see the warning in Step 7.
   - **Auto-assign public IP**: **Enable**
   - **Security group**: **Select existing** → **`sg-edge`**
7. **Configure storage**: `20` GiB, `gp3`.
8. **Launch instance**.

**✓ Check:** both instances show **Running** and **2/2 checks passed** (give it a
minute or two).

To SSH into box-edge, use its **Public IPv4 address** from the Details tab — it has
no Elastic IP, so this address changes if you ever stop the instance:

```bash
ssh -i ~/.ssh/signal-prod.pem ubuntu@<box-edge public IP>
```

### Add swap to both servers

2 GiB of RAM plus a Java process or a Node build is tight. Swap prevents the
out-of-memory killer from ending things mid-build. Run this **on each server**:

```bash
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
free -h        # the Swap row should now show 2.0Gi
```

---

## Step 9 — DNS record for the broker

1. GoDaddy → your domain → **DNS** → **Add New Record**:

| Field | Value |
| --- | --- |
| Type | **A** |
| Name | **`kafka`** — just that (Step 3's gotcha) |
| Value | your `ELASTIC IP` |
| TTL | 600 seconds |

2. **Save**.

**✓ Check**, from your own machine:

```bash
dig +short kafka.yourdomain.com     # must print your Elastic IP
```

If it prints nothing: wait out the TTL, then check the Name field for a doubled
domain.

> **Do not start Step 10 until this resolves.** Step 10.2 gets a TLS certificate by
> having Let's Encrypt connect to this exact name. No DNS, no certificate, and the
> rest of the pipeline cannot work.

---

## Step 10 — Install and configure Kafka

SSH into **box-kafka**. Everything in this step runs there.

### 10.1 Install

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y certbot kcat

# Java: Kafka 4.x BROKERS require Java 17 or newer. 21 is a safe choice; if the
# package is not in your Ubuntu release, `default-jre-headless` will do.
sudo apt install -y openjdk-21-jre-headless || sudo apt install -y default-jre-headless

KAFKA_VER=4.3.1
curl -fsSL "https://dlcdn.apache.org/kafka/${KAFKA_VER}/kafka_2.13-${KAFKA_VER}.tgz" -o /tmp/kafka.tgz
sudo tar -xzf /tmp/kafka.tgz -C /opt && sudo mv "/opt/kafka_2.13-${KAFKA_VER}" /opt/kafka
sudo useradd -r -s /sbin/nologin kafka
sudo mkdir -p /var/lib/kafka && sudo chown -R kafka:kafka /var/lib/kafka /opt/kafka
```

`2.13` in the filename is the Scala version Kafka was built with, not a Kafka
version — `kafka_2.13-4.3.1.tgz` is the only binary build published for 4.3.1.

**✓ Check:** `java -version` prints **17 or higher**, and `ls /opt/kafka/bin` lists
scripts. Java 11 or older will not run a Kafka 4.x broker at all.

> **If you use a newer Kafka than 4.3.1**, the only lines below that could need
> attention are the two KRaft controller settings in 10.3 — check the release notes
> for `controller.quorum.*`. Everything else here is stable across 4.x.

### 10.2 Get the TLS certificate

```bash
sudo certbot certonly --standalone -d kafka.yourdomain.com
```

Answer the email prompt and accept the terms. This works only because Step 9's DNS
record resolves and `sg-kafka` allows port 80.

Kafka reads PEM files directly (2.7+), so there is no Java keystore to build:

```bash
sudo mkdir -p /etc/kafka/ssl
sudo bash -c 'cat /etc/letsencrypt/live/kafka.yourdomain.com/fullchain.pem \
  /etc/letsencrypt/live/kafka.yourdomain.com/privkey.pem \
  > /etc/kafka/ssl/server.pem'
sudo chmod 600 /etc/kafka/ssl/server.pem
sudo chown kafka:kafka /etc/kafka/ssl/server.pem
```

**Certificates expire every 90 days and Kafka does not reload them by itself.** Set
the renewal hook now, or ingestion breaks silently in three months:

```bash
sudo tee /etc/letsencrypt/renewal-hooks/deploy/kafka.sh >/dev/null <<'EOF'
#!/bin/bash
cat /etc/letsencrypt/live/kafka.yourdomain.com/fullchain.pem \
    /etc/letsencrypt/live/kafka.yourdomain.com/privkey.pem \
    > /etc/kafka/ssl/server.pem
chmod 600 /etc/kafka/ssl/server.pem
chown kafka:kafka /etc/kafka/ssl/server.pem
systemctl restart kafka
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/kafka.sh
```

**✓ Check:** `sudo ls -l /etc/kafka/ssl/server.pem` exists and is owned by `kafka`.

### 10.3 Write the configuration

Edit `/opt/kafka/config/server.properties` (`sudo nano` is fine). Replace the whole
file with this, **substituting your `BOX-KAFKA PRIVATE IP`** and your domain:

```properties
process.roles=broker,controller
node.id=1
# Kafka 4.x: the old `controller.quorum.voters=1@localhost:9093` is DEPRECATED
# (KRaft version 1 replaced it). Use bootstrap.servers, and make this node the
# sole voter with `--standalone` at format time in 10.4.
controller.quorum.bootstrap.servers=localhost:9093

listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:SASL_SSL,CONTROLLER:PLAINTEXT
advertised.listeners=INTERNAL://<BOX-KAFKA-PRIVATE-IP>:9092,EXTERNAL://kafka.yourdomain.com:9094
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER

sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.external.sasl.enabled.mechanisms=SCRAM-SHA-512

ssl.keystore.type=PEM
ssl.keystore.location=/etc/kafka/ssl/server.pem

log.dirs=/var/lib/kafka
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

log.retention.hours=168
log.retention.bytes=-1
```

> **`advertised.listeners` is the line that breaks this deployment.** It is not what
> the broker listens on — it is what the broker *tells clients to connect to* after
> they first make contact. Both halves matter:
>
> - **`INTERNAL` must be box-kafka's private IP**, never `localhost`. If it said
>   `localhost`, box-edge would be told "now talk to localhost", connect to
>   *itself*, and fail.
> - **`EXTERNAL` must be `kafka.yourdomain.com`**, never an IP. The certificate was
>   issued for that name, and the client checks that the name matches.
>
> Symptom of either mistake, seen later in Step 12: a live consumer that has read
> **0** messages, with a connection error repeating every ~31 seconds.

`default.replication.factor=1` because this is a single broker. Asking for more
copies than you have brokers makes every topic creation fail outright. Durability
here comes from ClickHouse's copy plus the 7-day retention above.

Cap Java's memory, or it will try to take most of your 2 GiB:

```bash
echo 'KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"' | sudo tee /etc/default/kafka
```

### 10.4 Initialise storage

Kafka has to write its metadata layout once, before first start:

```bash
CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
echo "$CLUSTER_ID"          # note it down; harmless but useful if you ever debug

sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
  --cluster-id "$CLUSTER_ID" \
  --standalone \
  --config /opt/kafka/config/server.properties
```

**`--standalone` is the Kafka 4.x way to say "this one node is the entire
controller quorum".** It writes an initial snapshot naming this node as the sole
voter, which is what replaces the deprecated `controller.quorum.voters` line.
Without it, a 4.x node configured only with `controller.quorum.bootstrap.servers`
has no quorum to join and will not start.

**✓ Check:** prints `Formatting metadata directory ...` and exits 0. If it
complains the directory is already formatted, you have run this before — that is
fine, skip ahead.

> **You only format once, ever.** Re-running it on a live broker is how you erase
> the topic. If you genuinely need to start over, stop Kafka and
> `sudo rm -rf /var/lib/kafka/*` first — and understand that this discards every
> message not yet in ClickHouse.

### 10.5 Run it as a service

```bash
sudo tee /etc/systemd/system/kafka.service >/dev/null <<'EOF'
[Unit]
Description=Apache Kafka (KRaft)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kafka
EnvironmentFile=/etc/default/kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload && sudo systemctl enable --now kafka
sudo journalctl -u kafka -f
```

**✓ Check:** the log reaches **`Kafka Server started`**. Press Ctrl-C to stop
watching. If it crash-loops, the error is almost always a typo in
`server.properties` — read the first exception, not the last.

### 10.6 Create the ClickHouse login

This is the username and password ClickHouse Cloud will authenticate with. Create
it **after** the broker is running, over the local plaintext listener — which needs
no credentials, and is why it is reachable only from this machine.

```bash
openssl rand -base64 24        # the password — COPY IT to your scratch sheet now
```

Then, substituting it in:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type users --entity-name clickhouse \
  --add-config 'SCRAM-SHA-512=[password=<THE-PASSWORD>]'
```

Save it as `KAFKA SCRAM PASSWORD`. **Step 12 needs it**, and there is no way to read
it back out of Kafka afterwards — only overwrite it by re-running the same command
with a new value.

Do not reuse `INTERNAL_SETTLE_SECRET` for this.

**✓ Check:**

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --describe --entity-type users --entity-name clickhouse
```

Prints a line mentioning `SCRAM-SHA-512`. It shows the iteration count, never the
password.

> The broker starts happily with no users defined — SCRAM credentials are checked
> when a client authenticates, not at boot. So a running broker before this step is
> normal, not a sign something is wrong.

### 10.7 Create the topic

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic signals --partitions 1 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic signals | head -1
```

**✓ Check:** prints `PartitionCount: 1   ReplicationFactor: 1`.

<details>
<summary><b>One partition is right here — what you'd watch to change that</b></summary>

At your volume (peak ~4 MB/s for ~2 seconds) one partition is ample. Two costs you
are accepting, both measurable:

1. **No ingest parallelism.** One partition means at most one useful ClickHouse
   consumer, so if `num_messages_read` starts falling behind the produce rate there
   is no knob to turn — you must grow the topic first.
2. **Head-of-line blocking.** One slow or oversized message stalls everything
   behind it. With 3 partitions it would stall a third of the stream.

**Growing 1 → 3 is unusually cheap in this pipeline.** The normal objection is that
adding partitions rehashes `customerId` → partition and splits a customer's ordered
stream. That does happen; it costs nothing here, because **nothing reads the topic
in order**. The materialized view inserts into `signal_log`, which is
`ORDER BY (received_at, signal_id)` and dedups on `signal_id`; the dispatcher
windows on `ingested_at` and bills on `received_at`; settle is idempotent on
`signalId`. No stateful per-customer aggregation exists anywhere.

**The one real hazard**, and the reason `kafka_auto_offset_reset = 'earliest'` is
set in Step 12: a brand-new partition has no committed offset, so
`auto.offset.reset` decides where its consumer starts — and the default is
`latest`. The producer refreshes topic metadata on its own schedule and can start
writing to a new partition *before* ClickHouse rebalances onto it. On `latest`,
everything in that gap is skipped permanently. `earliest` closes it, costs nothing
until the day you grow, and must be in place **beforehand**.

To grow: `kafka-topics.sh --alter --topic signals --partitions 3`. Partition count
can never be **lowered**. And 3 partitions would not be high availability either —
they would all live on this one broker.
</details>

### 10.8 Prove the public listener works — do not skip this

This is the single most likely thing to be broken, and diagnosing it later through
ClickHouse's error messages is miserable.

First, temporarily let yourself in: **`sg-kafka`** → **Edit inbound rules** → add
**Custom TCP 9094, Source: My IP** → **Save**. (It's locked to ClickHouse, which
isn't you.)

Then from **your own machine**, not the server:

```bash
kcat -b kafka.yourdomain.com:9094 -L \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=clickhouse \
  -X sasl.password='<KAFKA SCRAM PASSWORD>'
```

**✓ Check:** you see the `signals` topic with **1 partition**.

If not, check in this exact order:

| Symptom | Look at |
| --- | --- |
| connection times out | `sg-kafka` port 9094 — did the My IP rule save? |
| "could not resolve" | `dig +short kafka.yourdomain.com` |
| connects then fails oddly | `EXTERNAL` in `advertised.listeners` |
| certificate/hostname error | the cert was issued for a different name |
| authentication failure | the SCRAM password, retyped |

**When it passes, remove two rules from `sg-kafka`:**

1. your **My IP on 9094** rule, and
2. the **port 80** rule from Step 5.4.

> Certificate renewal needs port 80 again in 90 days. Either re-open it then, or
> switch the hook to DNS-01 validation.

---

## Step 11 — Install and run the edge

SSH into **box-edge**. Everything here runs there.

### 11.1 Node

Try Ubuntu's own package first — on 26.04 it is recent enough and avoids a
third-party repository:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y nodejs npm git jq
node --version
```

**This project needs Node 20.6 or newer** (it uses `--env-file-if-exists` and
`node --test`). Node 22+ is what it is developed against.

**If `node --version` is older than 20.6**, add NodeSource instead:

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install -y nodejs
node --version
```

> On a brand-new Ubuntu release, NodeSource may not recognise the codename yet and
> the script will say so. If that happens and Ubuntu's own package is too old, use
> `nvm` as the fallback:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
> exec $SHELL -l && nvm install 22
> ```
> A systemd unit cannot see an nvm shim, so if you go this route, point
> `ExecStart` at the absolute path from `which node` instead of `/usr/bin/node`.

**✓ Check:** `node --version` prints 20.6 or newer.

### 11.2 Clone and build

```bash
sudo mkdir -p /srv && sudo chown ubuntu:ubuntu /srv
git clone <your-repo-url> /srv/events-microservice
cd /srv/events-microservice && npm ci && npm run build
```

**✓ Check:** `ls dist/server.js` exists.

### 11.3 Configuration

Create `/srv/events-microservice/.env`:

```bash
NODE_ENV=production
HOST=0.0.0.0
PORT=3000
LOG_LEVEL=info

# box-kafka's PRIVATE IP from Step 7. Plaintext, but private to your VPC
# and restricted by sg-kafka.
KAFKA_BROKERS=<BOX-KAFKA-PRIVATE-IP>:9092
KAFKA_CLIENT_ID=signal-edge
KAFKA_TOPIC=signals
# FALSE — IAM auth is for MSK, and you are not using MSK.
KAFKA_USE_IAM=false

BODY_BYTES=65536

# Dispatcher only. The edge ignores all of this.
CLICKHOUSE_URL=https://<your-service>.clickhouse.cloud:8443
CLICKHOUSE_DATABASE=signals
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<from Step 2>

PAYMENTS_URL=https://your-payments-app.vercel.app
INTERNAL_SETTLE_SECRET=<must match the payments deployment>

DISPATCH_WINDOW_MS=180000
DISPATCH_MAX_ROWS=100000
DISPATCH_TIMEOUT_MS=310000
DISPATCH_GZIP=true
DISPATCH_SINCE=
DISPATCH_UNTIL=
```

```bash
chmod 600 /srv/events-microservice/.env
```

> **`.env` is read by the npm scripts, never by the code** — `src/config.ts` only
> reads `process.env`. A systemd unit that runs `node` directly **must** set
> `EnvironmentFile=`, or the process starts with no configuration at all.

```bash
sudo tee /etc/systemd/system/edge.service >/dev/null <<'EOF'
[Unit]
Description=Signal edge (Fastify)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/srv/events-microservice
EnvironmentFile=/srv/events-microservice/.env
ExecStart=/usr/bin/node dist/server.js
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload && sudo systemctl enable --now edge
sudo systemctl status edge
```

> There is no `After=kafka.service` here — Kafka is on a different machine and
> systemd cannot order across machines. That is fine: if Kafka is unreachable the
> edge answers **502 `QUEUE_UNAVAILABLE`** instead of falsely accepting a signal.
> An accepted signal is billable, so it is never acknowledged unless it truly
> reached the topic.

**✓ Check** — both of these, on box-edge:

```bash
curl -s localhost:3000/health | jq

curl -s -X POST localhost:3000/api/v1/signal \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <a real cnk_ key>' \
  -d '{"customerId":"cus_test","inputTokens":1200,"outputTokens":340}' | jq
```

The second must return **202** with a `signalId`. A **502 `QUEUE_UNAVAILABLE`**
means one of exactly two things: `sg-kafka`'s port 9092 rule doesn't have `sg-edge`
as its source, or `INTERNAL` in `advertised.listeners` isn't the private IP.

---

## Step 12 — ClickHouse schema

In the ClickHouse Cloud **SQL console** (left sidebar of your service).

**Order matters: create `signal_log` first.** The materialized view starts draining
the moment it exists, so its destination table must already be there.

1. Open `docker/clickhouse/init/01-schema.sql` from this repo.
2. Run the `CREATE DATABASE` and the `signals.signal_log` block **unchanged**.
3. Then run `kafka_signals` with the `SETTINGS` block **replaced** — the committed
   one points at `kafka:29092`, a Docker address that does not exist in AWS:

```sql
CREATE TABLE IF NOT EXISTS signals.kafka_signals
(
  raw String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list       = 'kafka.yourdomain.com:9094',
  kafka_topic_list        = 'signals',
  kafka_group_name        = 'clickhouse-signal-log',
  kafka_format            = 'JSONAsString',
  kafka_num_consumers     = 1,
  kafka_security_protocol = 'sasl_ssl',
  kafka_sasl_mechanism    = 'SCRAM-SHA-512',
  kafka_sasl_username     = 'clickhouse',
  kafka_sasl_password     = '<KAFKA SCRAM PASSWORD>',
  kafka_auto_offset_reset = 'earliest';
```

4. Then run the `signal_log_mv` block **unchanged**.
5. Then run `docker/clickhouse/migrations/002_ingested_at_index.sql` and
   `003_kafka_auto_offset_reset.sql`.

**Leave `kafka_num_consumers = 1`.** With one partition, one consumer is the most
that can do any work.

> **Never run `SELECT` against `kafka_signals`.** ClickHouse will refuse with
> *Code 620 · Direct select is not allowed*, and that refusal is protecting you:
> reading the stream **consumes messages and commits offsets**, so every row you'd
> see is a row the materialized view never receives — permanently gone from the
> archive. **Never set `stream_like_engine_allow_direct_select`.** Query
> `signal_log` instead; it is a normal table with no side effects.

> **These settings cannot be edited later.** `ALTER TABLE … MODIFY SETTING` does
> not work on a `Kafka` engine table. To change the broker or password: drop
> `signal_log_mv` **first** (it holds the consumer), then `kafka_signals`, then
> recreate both. `signal_log` is untouched, and the group name is unchanged so
> offsets resume.

**✓ Check:**

```sql
SELECT count() FROM signals.signal_log;      -- includes Step 11's test signal

SELECT table, num_messages_read, last_poll_time, last_commit_time,
       exceptions.time, exceptions.text
FROM system.kafka_consumers;
```

You want `num_messages_read` above zero and `exceptions.text` empty. If you see a
live consumer with **0 messages** and a repeating connection error, go back to
Step 10.3 — `advertised.listeners`.

---

## Step 13 — Dispatch timers

On **box-edge**. The dispatcher is a one-shot program, not a daemon: each run reads
one time window of the archive, posts it to the payments app, and exits.

```bash
sudo cp /srv/events-microservice/deploy/systemd/*.service \
        /srv/events-microservice/deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dispatch.timer dispatch-reconcile.timer
sudo systemctl list-timers 'dispatch*'
journalctl -u dispatch -f -o cat
```

**✓ Check:** `list-timers` shows both timers with a *NEXT* time, and the journal
shows a run within a minute.

Exit codes are the interface: **0** sent or nothing to send, **1** transient (the
next tick is the retry — there is no backoff), **2** misconfigured (every tick will
fail identically until a human fixes it). Details:
[deploy/systemd/README.md](../deploy/systemd/README.md).

---

## Step 14 — Load balancer

### 14.1 Request the certificate

1. Top search bar → **Certificate Manager** → make sure the region is your `REGION`.
2. **Request a certificate** → **Request a public certificate** → **Next**.
3. **Fully qualified domain name**: `api.yourdomain.com`
4. **Validation method**: **DNS validation**.
5. **Request**.
6. Click into the certificate. Under *Domains* you'll see a **CNAME name** and
   **CNAME value**.
7. In GoDaddy, **Add New Record** → Type **CNAME**:
   - **Name**: the CNAME name **with your domain stripped off** — e.g. if AWS shows
     `_a1b2c3.api.yourdomain.com`, enter `_a1b2c3.api`
   - **Value**: paste exactly as shown; if GoDaddy rejects the trailing dot, remove it
   - **TTL**: 600
8. Wait. **✓ Check:** the certificate status becomes **Issued** (5–30 minutes).

> Leave that CNAME in place forever — ACM re-reads it to renew automatically.

### 14.2 Create the target group

1. **EC2** → left sidebar, under *Load Balancing* → **Target Groups** →
   **Create target group**.
2. **Choose a target type**: **Instances**.
3. **Target group name**: `tg-edge`
4. **Protocol / Port**: **HTTP** / **3000**
5. **VPC**: your `VPC ID`
6. Expand **Health checks**:
   - **Health check protocol**: HTTP
   - **Health check path**: `/health`
7. **Next**.
8. In *Register targets*, tick **`box-edge`**, confirm **Port 3000**, click
   **Include as pending below**.
9. **Create target group**.

**✓ Check:** the target group's *Targets* tab shows `box-edge` as **healthy** after
30–60 seconds. If it stays **unhealthy**, `sg-edge`'s port 3000 rule must have
`sg-alb` as its source (Step 5.3).

### 14.3 Create the load balancer

1. **EC2** → *Load Balancing* → **Load Balancers** → **Create load balancer**.
2. **Application Load Balancer** → **Create**.
3. **Name**: `alb-edge`
4. **Scheme**: **Internet-facing**. **IP address type**: IPv4.
5. **Network mapping**: select your **VPC**, then tick **two** Availability Zones
   and choose **`SUBNET A`** and **`SUBNET B`** respectively.
   *(This is why Step 4 had you note two subnets — an ALB requires two AZs.)*
6. **Security groups**: **`sg-alb`**. Remove `default` if it's pre-selected.
7. **Listeners and routing**:
   - **Protocol**: **HTTPS**, **Port**: **443**
   - **Default action**: forward to **`tg-edge`**
8. **Secure listener settings** → **Certificate**: the ACM certificate from 14.1.
9. **Create load balancer**. Wait for state **Active** (2–3 minutes).
10. Copy its **DNS name** to your scratch sheet as `ALB DNS NAME`.

**✓ Check:**

```bash
curl -sk https://<ALB DNS NAME>/health | jq
```

Returns your health payload. `-k` skips certificate checking, which is expected
here — the certificate is for `api.yourdomain.com`, not the ALB's own name.

---

## Step 15 — CloudFront

### 15.1 The certificate CloudFront will use

CloudFront reads certificates **only** from `us-east-1`, whatever region the rest of
your stack is in.

> **If your `REGION` is already `us-east-1`: skip this section entirely.** The
> certificate you made in Step 14.1 is already in the right place — reuse it in 15.2
> and you are done. One certificate, one GoDaddy CNAME. This is a real perk of
> deploying in `us-east-1` and it partly offsets the ~17 % that `ap-south-1` would
> have saved you.

**Otherwise**, you need a second certificate for the same hostname:

1. **Certificate Manager** → **switch the region dropdown to `us-east-1`**.
2. Request a public certificate for `api.yourdomain.com`, DNS validation — exactly
   as in 14.1.
3. Add the new CNAME it gives you in GoDaddy, **alongside** the first one. They are
   different records; keep both.
4. **✓ Check:** status **Issued**.

### 15.2 Create the distribution

1. Top search bar → **CloudFront** → **Create distribution**.
2. **Origin domain**: your `ALB DNS NAME`.
3. **Protocol**: **HTTPS only**.
4. **Default cache behavior**:
   - **Viewer protocol policy**: **Redirect HTTP to HTTPS**
   - **Allowed HTTP methods**: **GET, HEAD, OPTIONS, PUT, POST, PATCH, DELETE**
   - **Cache policy**: **CachingDisabled**
   - **Origin request policy**: **AllViewer**
5. **Settings**:
   - **Alternate domain name (CNAME)**: `api.yourdomain.com`
   - **Custom SSL certificate**: the certificate from 15.1 — if the dropdown is
     empty, your certificate is not in `us-east-1`
6. **Create distribution**. Deployment takes ~5 minutes.
7. Copy the **Distribution domain name** (`dxxxxx.cloudfront.net`) to your scratch
   sheet.

> **Two of those settings are not optional, and both fail confusingly:**
>
> - **`AllViewer`** — without it CloudFront strips the `Authorization` header, and
>   every request arrives as **401 `API_KEY_MISSING`** even though your key is
>   correct.
> - **`CachingDisabled`** — caching a `POST /api/v1/signal` response would hand one
>   caller's `signalId` to a different caller.

### 15.3 Point your domain at it

1. GoDaddy → **Add New Record**:

| Field | Value |
| --- | --- |
| Type | **CNAME** |
| Name | **`api`** |
| Value | `dxxxxx.cloudfront.net` — no `https://`, no trailing slash |
| TTL | 600 |

2. **Save**.

**✓ Check:**

```bash
dig +short api.yourdomain.com     # shows cloudfront.net, then IP addresses
```

> **Why CNAME and not A?** CloudFront's IP addresses change constantly, so you point
> at its *name* and let AWS move it underneath. This only works on a subdomain — a
> CNAME on a bare domain is invalid DNS. That is exactly why the API lives at
> `api.yourdomain.com` rather than `yourdomain.com`.

---

## Step 16 — Verify the whole pipeline

```bash
BASE=https://api.yourdomain.com

curl -s -X POST $BASE/api/v1/signal \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <a real cnk_ key>' \
  -d '{"customerId":"cus_real","inputTokens":1200,"outputTokens":340}' | jq
```

Then walk the five hops. Each has its own place to look:

| Hop | Where to check |
| --- | --- |
| 1. CloudFront → ALB → edge | the response above: **202** with a `signalId` |
| 2. edge → Kafka | box-edge: `journalctl -u edge -f` |
| 3. Kafka → ClickHouse | SQL console: `SELECT * FROM signals.signal_log WHERE signal_id = '<id>'` |
| 4. dispatcher → settle | box-edge: `journalctl -u dispatch -f -o cat` |
| 5. settle → Postgres | the payments app's `SignalLog` / `SignalStatus` rows |

Now test the rejections. **These are contract, not accidents** — a caller may rely
on them:

```bash
# no auth header     -> 401 API_KEY_MISSING
curl -s -X POST $BASE/api/v1/signal \
  -H 'Content-Type: application/json' -d '{}' | jq

# wrong content-type -> 415 UNSUPPORTED_MEDIA_TYPE
curl -s -X POST $BASE/api/v1/signal \
  -H 'Content-Type: text/plain' -H 'Authorization: Bearer x' -d 'hi' | jq

# missing fields     -> 400 INVALID_BODY with issues[]
curl -s -X POST $BASE/api/v1/signal \
  -H 'Content-Type: application/json' -H 'Authorization: Bearer x' \
  -d '{"customerId":"c"}' | jq
```

Every rejection must **still carry `signalId` and `receivedAt`** — they are stamped
before anything can fail, so a caller can quote even a hard 404 back at you.

> **Expect hops 4 and 5 to fail for now.** The dispatcher's current design needs two
> changes on the payments side: settle must accept one call of arbitrary size (its
> 500-signal cap removed, its per-signal loop replaced by a bulk insert), and it
> must decompress a gzipped request body. Until both land, that failure is upstream
> — not your AWS wiring. Hops 1–3 are the real test of this deployment.

---

## Step 17 — Lock it down, then watch it

### 17.1 Close the ALB to the public

CloudFront is now the intended front door, so the ALB should only accept traffic
from CloudFront.

1. **EC2** → **Security Groups** → **`sg-alb`** → **Edit inbound rules**.
2. On the 443 rule, change **Source** from `Anywhere-IPv4` to **Custom**, then
   search for and select the managed prefix list
   **`com.amazonaws.global.cloudfront.origin-facing`**.
3. **Save rules**.

**✓ Check:** `curl -sk https://<ALB DNS NAME>/health` now times out, while
`curl -s https://api.yourdomain.com/health` still works. Without this,
anyone who discovers the ALB's DNS name bypasses CloudFront entirely.

### 17.2 Confirm the firewalls are tight

On **`sg-kafka`**, verify:

- **no** port 80 rule (removed in 10.8)
- **no** `0.0.0.0/0` anywhere
- port 9094 lists only ClickHouse's egress IPs
- port 9092's source is `sg-edge`
- port 22's source is your IP only

### 17.3 Alarms worth having on day one

| Watch for | Why it matters |
| --- | --- |
| `run.capped` in the dispatch logs | **DATA WAS LOST.** `DISPATCH_MAX_ROWS` is an out-of-memory guard, not a batch size — hitting it means the window held more than one run can carry, and the next window has already moved past some rows |
| `dispatch.body_near_limit` | approaching Vercel's 4.5 MB request cap; crossing it is a hard 413 for the whole window at once |
| dispatch failures, ≥3 in a row | the 3× window overlap covers one or two failed runs; three is a real outage, which is what the hourly reconciliation timer exists for |
| `system.kafka_consumers` exceptions non-empty | ingestion has stopped and the archive is falling behind silently |
| EC2 **CPU credit balance**, both servers | `t4g.small` is burstable — exhausting credits throttles you to a fraction of normal speed |
| ALB 5xx count and target health | the edge is down or unreachable |

For an outage longer than the reconciliation window, replay a specific period with
`DISPATCH_SINCE` / `DISPATCH_UNTIL`. No code change and no cursor to rewind.

---

## If something is wrong

| Symptom | Almost always |
| --- | --- |
| A console list is empty | wrong **region** in the top-right dropdown |
| SSH hangs | your IP changed — update port 22 in the security group |
| SSH *permission denied (publickey)* | the username is `ubuntu`; and `chmod 400` the `.pem` |
| `t4g.small` missing at launch | AMI architecture wasn't switched to **64-bit Arm** |
| `dig` returns nothing | GoDaddy **Name** field has the domain typed twice |
| certbot fails | DNS not resolving yet, or port 80 not open on `sg-kafka` |
| Kafka won't start | typo in `server.properties` — read the **first** exception |
| Edge returns 502 `QUEUE_UNAVAILABLE` | `sg-kafka`:9092 source isn't `sg-edge`, or `INTERNAL` advertised address is wrong |
| ClickHouse consumer alive, 0 messages | `advertised.listeners` **`EXTERNAL`**, or the 9094 egress-IP rules |
| Target group stuck **unhealthy** | `sg-edge`:3000 source must be `sg-alb`, not an IP |
| Everything 401 through CloudFront | origin request policy isn't **AllViewer** |

---

## Condensed checklist

```
 0  learn the nine words; understand you use the DEFAULT VPC
 1  region · budget alert $50 · keypair signal-prod.pem (chmod 400) · curl ifconfig.me
 2  ClickHouse Cloud service -> endpoint, password, EGRESS IPS from static-ips.json
 3  GoDaddy DNS panel; Name field is RELATIVE; TTL 600
 4  VPC console -> default VPC id; two subnet ids (A = both boxes, B = ALB only)
 5  sg-alb (443 open) · sg-edge (3000 from sg-alb, 22 me) · sg-kafka (9092 from
    sg-edge, 9094 from ~6 CH ips, 22 me, 80 temporarily)
 6  allocate 1 Elastic IP
 7  launch box-kafka: Ubuntu 24.04 ARM, t4g.small, SUBNET A, sg-kafka, 30 GiB
    -> associate EIP -> record PRIVATE IP -> ssh works
 8  launch box-edge: same but SUBNET A, sg-edge, 20 GiB -> swap on both boxes
 9  GoDaddy A record: kafka -> EIP -> dig confirms
10  box-kafka: install (Kafka 4.3.1, Java 17+) -> certbot + renewal hook
    -> server.properties (PRIVATE IP! bootstrap.servers, not voters)
    -> format --standalone -> systemd -> SCRAM user -> topic (1 partition)
    -> kcat proves 9094 -> close port 80 and your 9094 rule
11  box-edge: node -> clone + build -> .env (KAFKA_BROKERS = private ip)
    -> edge.service -> curl localhost:3000 returns 202
12  ClickHouse SQL: signal_log FIRST -> kafka_signals (your domain, sasl_ssl)
    -> signal_log_mv -> migrations 002 + 003 -> count() rises
13  box-edge: dispatch.timer + dispatch-reconcile.timer
14  ACM cert (your region) + GoDaddy CNAME -> target group :3000 /health
    -> ALB HTTPS 443 across two AZs -> curl -k the ALB
15  cert for CloudFront (must be us-east-1; reuse step 14's if REGION=us-east-1)
    -> CloudFront: CachingDisabled + AllViewer
    -> GoDaddy CNAME api -> dxxxx.cloudfront.net
16  verify five hops + three rejection modes
17  sg-alb 443 -> CloudFront prefix list; confirm sg-kafka tight; set alarms
```
