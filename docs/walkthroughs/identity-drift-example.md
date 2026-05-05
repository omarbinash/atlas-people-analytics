# Walkthrough: Identity Drift Recovery

This walkthrough describes the class of case Atlas is built to handle. The data
is synthetic, but the failure mode is common in real People Analytics systems.

## Scenario

A fabricated employee enters the applicant tracking system with a personal
email and preferred first name. After hire, HRIS creates the official employee
record with legal name, work email, DOB, and employment dates. Later systems
pick up different slices:

- payroll carries legal-name payroll spells
- CRM may use a preferred or shortened name
- DMS and ERP may share a user bridge with a local email pattern
- HRIS can issue a new employee ID after rehire or contractor-to-FTE conversion

Without a canonical person key, these rows can be counted as different people.

## Resolution Flow

1. `int_identity_source_nodes` normalizes source records into a shared identity
   grain without changing the synthetic source data.
2. Pass 1 resolves high-confidence hard anchors such as personal email and work
   email local part.
3. Pass 2 uses normalized name roots, DOB, and hire-date proximity for records
   that lack a direct email bridge.
4. Pass 3 recovers harder company-domain and last-name-token matches.
5. `int_canonical_person` emits the stable canonical ID.
6. `int_stewardship_queue` receives unresolved or ambiguous rows.

## Why The Queue Matters

The queue is a product feature, not a defect. In HR analytics, a bad merge can
corrupt compensation, attrition, tenure, and performance attribution. Atlas
therefore prefers a visible unresolved queue over an unsafe automatic match.

## Where To Look

- `dbt_project/models/intermediate/int_identity_source_nodes.sql`
- `dbt_project/models/intermediate/int_identity_pass_1_hard_anchors.sql`
- `dbt_project/models/intermediate/int_identity_pass_2_name_dob_hire.sql`
- `dbt_project/models/intermediate/int_identity_pass_3_email_domain.sql`
- `dbt_project/models/intermediate/int_canonical_person.sql`
- `dbt_project/models/intermediate/int_stewardship_queue.sql`

