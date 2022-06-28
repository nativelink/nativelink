# Turbo Cache's Terraform Deployment
This directory contains a reference/starting point on creating a full AWS terraform deployment of Turbo Cache's cache and remote execution system.

## Prerequisites - Setup Hosted Zone / Base Domain
You are required to first setup a Route53 Hosted Zone in AWS. This is because we will generate SSL certificates and need a domain to register them under.

1. Login to AWS and go to [Route53](https://console.aws.amazon.com/route53/v2/hostedzones)
2. Click `Create hosted zone`
3. Enter a domain (or subdomain) that you plan on using as the name and ensure it's a `Public hosted zone`
4. Click into the hosted zone you just created and expand `Hosted zone details` and copy the `Name servers`
5. In the DNS server that your domain is currently hosted under (it may be another Route53 hosted zone) create a new `NS` record with the same domain/subdomain that you used in Step 3. The value should be the `Name servers` from Step 4

It may take a few mins to propagate

## Terraform Setup
1. [Install terraform](https://www.terraform.io/downloads)
2. Open terminal and run `terraform init` in this directory
3. Run `terraform apply -var base_domain=INSERT_DOMAIN_NAME_YOU_SETUP_IN_PREREQUISITES_HERE`

It will take some time to apply, when it is finished everything should be running. The endpoints are:
```
CAS: grpcs://cas.INSERT_DOMAIN_NAME_YOU_SETUP_IN_PREREQUISITES_HERE
Scheduler: grpcs://scheduler.INSERT_DOMAIN_NAME_YOU_SETUP_IN_PREREQUISITES_HERE
```

As a reference you should be able to compile this project using bazel with something like:
```sh
bazel test //... \
    --remote_cache=grpcs://cas.INSERT_DOMAIN_NAME_YOU_SETUP_IN_PREREQUISITES_HERE \
    --remote_executor=grpcs://scheduler.INSERT_DOMAIN_NAME_YOU_SETUP_IN_PREREQUISITES_HERE
```

## Server configuration
![Turbo Cache AWS Terraform Diagram](https://user-images.githubusercontent.com/1831202/176286845-ff683266-3f23-489c-b58a-3eda49e484be.png)

## Instances
All instances use the same configuration for the AMI. There are technically two AMI's but only because by default this solution will spawn workers for x86 servers and ARM servers, so two AMIs are required.

### CAS
The CAS is only used as a public interface to the S3 data. All the services will talk to S3 directly, so they don't need to talk to the CAS instance.

#### More optimal configuration
You can reduce cost and increase reliability by moving the CAS onto the same machine that invokes the remote execution protocol (like bazel). Then point the configuration to `localhost` and it will translate the S3 calls into the Bazel Remote Execution Protocol API.
In bazel you can do this by making an executable file at `tools/bazel` in your WORKSPACE directory. This file can be a scripting language (like bash or python), then start the local proxy before starting bazel as a background service and then invoke the actual bazel executable with the proper flags configured.

### Scheduler
The scheduler is currently the only single point of failure in the system. We currently only support one scheduler at a time.
The workers will lookup the scheduler in a Route53 DNS record set by a lambda function that is configured to execute every time an instance change happens on the auto-scaling group the scheduler is under.
We don't use a load balancer here mostly for cost reaons and the fact that there's no real gain from using one, since we don't want/need to encrypt our data since we are using it all inside the VPC.

### Workers
Worker instances in this confugration (but can be changed) will only spawn 1 or 2 CPU machines all with NVMe drives. This is also for cost reasons, since NVMe drives are much faster and often cheaper than EBS volumes.
When the instance spawns it will lookup the available properties of the node and notify the scheduler. For example, if the instance has 2 cores it will let the scheduler know it has two cores.

## Security
The security permissions of each instance group is very strict. The major volnerabilitys are that the instances by default are all public ip instances and we allow incoming traffic on all instances to port 22 (SSH) for debugging reaons.

The risk of a user using this configuration in production is quite high and for this reason we don't allow the two S3 buckets (access logs for ELBs and S3 CAS bucket) to be deleted if they have content.
If you would like to use `terraform destroy`, you will need to manually purge these buckets or change the terraform files to force destroy them.
Taking the safer route seemed like the best route, even if it means the default developer life is slightly more difficult.

## Future work / TODOs
* Currently we never delete S3 files. Depending on the configuration this needs to be done carefully. Likely the best approach is with a service that runs constantly.
* Auto scaling up the instances is not confugred. An endpoint needs to be made so that a parsable (like json) feed can be read out of the scheduler through a lambda and publish the results to `CloudWatch`; then a scaling rule should be made for that ASG.

## Useful tips
You can add `-var terminate_ami_builder=false` to the `teraform apply` command and it will make it easier to modify/apply/test your changes to these `.tf` files.
This command will cause the AMI builder instances to not be terminated, which costs more money, but makes it so that terraform will not create a new AMI each time you call the command.
