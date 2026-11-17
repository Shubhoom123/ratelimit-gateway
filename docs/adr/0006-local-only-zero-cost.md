# 0006. Run entirely locally, with no cloud deployment

Status: accepted

## Context

The original plan included deploying to AWS (ECS Fargate, an ALB, ElastiCache) with Terraform and publishing load-test numbers from that deployment. The author's AWS account has no free tier and no credits, and a cost of exactly zero was a hard requirement. A load balancer and managed Redis are billed by the hour, and AWS Budgets only send alerts, they do not stop spending.

## Decision

Keep the project fully local. Docker Compose runs two gateways, Redis, two backends, Prometheus and Grafana. The load tests run against that stack. No cloud resources are created. A Terraform foundation was drafted and discarded without being applied.

## Consequences

- Every claim in this repository is reproducible for free with `make up` and `make test`.
- There are no numbers with real network distance between the gateway and Redis, and no managed-Redis behavior. The load-test report says so.
- Moving to a cloud later is possible: the gateway is stateless, takes config from a file or URL, and is built as one small image. A short one-off AWS session would be the way to do it, with `terraform destroy` straight afterwards.
