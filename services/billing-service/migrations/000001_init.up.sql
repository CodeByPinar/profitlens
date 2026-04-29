CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE IF NOT EXISTS billing.invoices (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES project.projects(id),
    amount_cents BIGINT NOT NULL,
    currency VARCHAR(3) DEFAULT 'TRY',
    status VARCHAR DEFAULT 'draft' CHECK (status IN ('draft', 'sent', 'paid', 'overdue')),
    issued_date DATE NOT NULL,
    due_date DATE NOT NULL,
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_billing_invoices_project_id ON billing.invoices(project_id);
CREATE INDEX IF NOT EXISTS idx_billing_invoices_status ON billing.invoices(status);
