CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS project;

CREATE TABLE IF NOT EXISTS project.clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id),
    name VARCHAR NOT NULL,
    country VARCHAR,
    currency VARCHAR(3) DEFAULT 'TRY',
    risk_score INTEGER DEFAULT 0 CHECK (risk_score BETWEEN 0 AND 100),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS project.projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL REFERENCES project.clients(id),
    name VARCHAR NOT NULL,
    type VARCHAR CHECK (type IN ('fixed', 'hourly', 'retainer')),
    budget_cents BIGINT DEFAULT 0,
    status VARCHAR DEFAULT 'active' CHECK (status IN ('active', 'completed', 'cancelled')),
    currency VARCHAR(3) DEFAULT 'TRY',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS project.time_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES project.projects(id),
    user_id UUID NOT NULL REFERENCES auth.users(id),
    hours NUMERIC(5,2) NOT NULL,
    hourly_rate_cents BIGINT NOT NULL,
    description TEXT,
    entry_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS project.expenses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES project.projects(id),
    category VARCHAR CHECK (category IN ('tool', 'outsource', 'meeting', 'other')),
    amount_cents BIGINT NOT NULL,
    currency VARCHAR(3) DEFAULT 'TRY',
    description TEXT,
    expense_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_project_clients_user_id ON project.clients(user_id);
CREATE INDEX IF NOT EXISTS idx_project_projects_client_id ON project.projects(client_id);
CREATE INDEX IF NOT EXISTS idx_project_time_entries_project_id ON project.time_entries(project_id);
CREATE INDEX IF NOT EXISTS idx_project_expenses_project_id ON project.expenses(project_id);
