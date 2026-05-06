# Wealthsimple 30-60-90 Plan

This plan is written for a Senior Analytics Developer joining a new People
Analytics function. It assumes the first mandate is to build a trusted employee
analytics foundation, not just ship isolated dashboards.

## First 30 Days: Understand And Stabilize

Primary goal: learn the people-data ecosystem and identify the highest-risk
definition gaps.

- Map the systems that create or modify employee identity, employment status,
  manager, department, location, compensation-relevant fields, and recruiting
  events.
- Identify authoritative sources by use case, not just by system name.
- Document first-pass metric definitions for headcount, hires, terminations,
  attrition, tenure, internal mobility, and recruiting funnel stages.
- Review existing HR, Finance, and People Ops reports to find reconciliation
  differences.
- Inventory sensitive fields and current access patterns.
- Establish a lightweight dbt testing baseline around source keys, accepted
  values, freshness, and high-risk joins.
- Partner with HRBPs and People Ops to learn which decisions are currently
  blocked by poor data trust.

Deliverables:

- source and ownership map
- metric definition draft
- initial data-quality risk register
- first reconciliation report for active headcount
- prioritized backlog for the employee identity spine

## Days 31-60: Build The Employee Spine

Primary goal: create the first trusted version of canonical employee identity
and point-in-time workforce history.

- Build canonical employee identity rules from the safest anchors first.
- Create a stewardship queue for records that should not be auto-merged.
- Implement SCD2 employee dimension logic for employment spells and changing
  attributes.
- Build a dated workforce fact for headcount and attrition analysis.
- Add tests for identity stability, one-row-per-grain expectations, SCD2
  continuity, and no-orphan source records.
- Reconcile active headcount and termination counts against HRIS and Finance
  control totals.
- Review edge cases with People Ops before expanding self-serve access.

Deliverables:

- canonical employee record v1
- stewardship queue v1
- `dim_employee` and workforce snapshot v1
- reconciliation notes with known differences
- dbt tests and model documentation

## Days 61-90: Govern And Expose Metrics

Primary goal: turn the foundation into safe, trusted, decision-ready metrics.

- Publish privacy-safe aggregate marts for the first priority People Analytics
  use cases.
- Create a semantic metric catalog with owners and versioned definitions.
- Add source freshness, reconciliation, and suppression observability to the
  normal operating workflow.
- Expose controlled stakeholder surfaces through BI, API, or lightweight apps,
  depending on team needs.
- Establish review practices for metric changes and identity stewardship.
- Document what should remain restricted to People Analytics versus what can
  become self-serve.
- Define the next roadmap: workforce planning, recruiting analytics, surveys,
  compensation analysis, or manager effectiveness based on leadership priority.

Deliverables:

- governed headcount and attrition marts
- stakeholder-facing metric surface
- privacy and access-control pattern
- metric ownership catalog
- operating cadence for data-quality review
- 6-month roadmap

## Management Conversation

The point of this plan is not to disappear into infrastructure. It is to create
trust quickly, then expand self-serve analytics only where the definitions,
identity logic, and privacy controls can support it.

Strong interview phrasing:

> In the first 90 days I would want Wealthsimple to have fewer people arguing
> about which headcount number is right, and more people using the same governed
> foundation to make workforce decisions.
