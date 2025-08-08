# Week 5 Data Pipeline Maintenance

### Homework assignement:

Imagine you're in a group of 4 data engineers, you will be in charge of creating the following things:

You are in charge of managing these 5 pipelines that cover the following business areas:
 
- Profit
  - Unit-level profit needed for experiments
  - Aggregate profit reported to investors
- Growth
  - Aggregate growth reported to investors
  - Daily growth needed for experiments
- Engagement 
  - Aggregate engagement reported to investors

You are in charge of figuring out the following things:

- Who is the primary and secondary owners of these pipelines?
- What is an on-call schedule that is fair
  - Think about holidays too!
- Creating run books for all pipelines that report metrics to investors
  - What could potentially go wrong in these pipelines?
  - (you don't have to recommend a course of action though since this is an imaginatione exercise)
  
Create a markdown file submit it!

### Homework execution:

# Runbooks

## Profit
### Profit Metrics

1. Pipeline Name: Profit Metrics

2. Types of data:
  
    a. Revenue from accounts

3. Owners:

    a. Primary Owner: Finance Team

    b. Secondary Owner: Business Analytics Team

4. Common Issues:
        
    a. Numbers don’t align with numbers on business reports, numbers need to be verified by an accountant

    b. Upstream schema change, coordinate with Finance Team

5. SLA’s:

    a. Numbers will be reviewed once a month by Finance Team

6. Oncall schedule
    a. No on call if pipeline fails, but pipeline will be debug by team during working hours

### Profit Experiments
1. Pipeline Name: Profit Experiments

2. Types of data:
  
    a. Revenue from accounts

    b. Assets and other services spendings

    c. Aggregated salaries by team/units

3. Owners:

    a. Primary Owner: Finance Team

    b. Secondary Owner: Data Engineering Team

4. Common Issues:

    a. Numbers don’t align with numbers on accounts/filings, numbers need to be verified by an accountant

    b. Experiments expenses are out-of-scale, verify cost calculation with cloud provider's cost

5. SLA’s:

    a. Numbers will be reviewed once a month by Finance Team

6. Oncall schedule
    a. Monitored by BI in Finance Team, team members rotate watching pipeline on weekly basis. If something breaks, it needs to be fixed

## Growth
### Growth Metrics

1. Pipeline Name: Growth Metrics

2. Types of data:
  
    a. Numbers of users with license MoM/YoY

3. Owners:

    a. Primary Owner: Accounts Team

    b. Secondary Owner: Business Analytics Team

4. Common Issues:
    
    a. Numbers don’t align with numbers on Accounts Team reports, numbers need to be verified by an accountant

    b. Upstream schema change, contact Accounts Team

5. SLA’s:

    a. Data will contain latest account statuses by end of week

6. Oncall schedule
    a. No on call if pipeline fails, but pipeline will be debug by team during working hours

### Growth Experiments

1. Pipeline Name: Growth Experiments

2. Types of data:
  
    a. Numbers of users with license increased

    b. Account stopped subscribing

    c. Account continued subscription for the next calendar year

3. Owners:

    a. Primary Owner: Accounts Team

    b. Secondary Owner: Data Engineering Team

4. Common Issues:
    
    a. Time series dataset, the current status of an account is missing since Accounts Team team forgot to update a step required to propagate changes in the downstream

5. SLA’s:

    a. Data will contain latest account statuses by end of week

6. Oncall schedule

    a. Monitored by BI in Accounts Team, team members rotate watching pipeline on weekly basis. If something breaks, it needs to be fixed

## Engagement
### Engagement Metrics

1. Pipeline Name: Engagement Metrics

2. Types of data:
  
    a. Engagement metrics come from clicks from all users using platforms in different teams

3. Owners: 

    a. Primary Owner: SWE Team

    b. Secondary Owner: Business Analytics Team

4. Common Issues:

    a. Sometimes data associated with click will arrive to kafka queue extremely late, much after the data has already been aggregated for a downstream pipeline
  
    b. If kafka goes down, all user clicks from website will not be sent to kafka, therefore not sent to the downstream metrics
    
    c. The same event will come through the pipeline multiple times, data must be de-duplicated

5. SLA’s:

    a. Data will arrive within 48hrs - if latest timestamp > the current timestamp - 48 hours, then the SLA is not met

    b. Issues will be fixed within 1 week

6. Oncall schedule
    
    a. One person on Data Engineering Team rotate watching pipeline on weekly basis, there is a contact on SWE team for questions

## Ownership Summary

| Pipeline                   | Primary Owner       | Secondary Owner       |
|----------------------------|---------------------|-----------------------|
| Profit Metrics             | Finance Team        | Business Analytics    |
| Profit Experiments         | Finance Team        | Data Engineering      |
| Growth Metrics             | Accounts Team       | Business Analytics    |
| Growth Experiments         | Accounts Team       | Data Engineering      |
| Engagement Metrics         | SWE Team            | Business Analytics    |

**Rationale:**  
- Profit pipelines are owned by **Finance** due to domain expertise.  
- Growth pipelines are owned by **Accounts** due to domain expertise. 
- Engagement pipeline are owned by **SWE Team** due to technical expertise for data ingestion and Kafka pipelines. 
- Metrics pipelines remain under **Business Analytics**, as they handle reporting trends, with DE as backup.
- Experiments pipeline have **Data Engineering** secondary ownership, as they require technical troubleshooting.

## On-call Schedule

**Principles:**
- Fair weekly rotation among 4 data engineers.
- Primary owner team takes lead; secondary owner assists if escalation is needed.
- Holidays covered by swap agreements within the rotation.
- Out-of-hours incidents: critical investor-reporting pipelines trigger pager alerts; non-critical ones are handled next business day unless SLA breach is imminent.

**Rotation Table Example:**

| Week  | Engineer A | Engineer B | Engineer C | Engineer D |
|-------|------------|------------|------------|------------|
| 1     | Profit Metrics & Experiments | Growth Metrics & Experiments | Engagement | Backup for all |
| 2     | Growth Metrics & Experiments | Engagement | Profit Metrics & Experiments | Backup for all |
| 3     | Engagement | Profit Metrics & Experiments | Growth Metrics & Experiments | Backup for all |
| 4     | Backup for all | Engagement | Profit Metrics & Experiments | Growth Metrics & Experiments |

**Holiday Coverage:**  
If an engineer is on leave, they swap weeks in advance with another engineer. The rotation restarts at the top after week 4.