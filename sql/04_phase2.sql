-- Phase 2: triage columns + earnings workflow (safe for re-run)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='feat' AND table_name='feat_returns' AND column_name='vol_7d') THEN
    ALTER TABLE feat.feat_returns ADD COLUMN vol_7d NUMERIC;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='feat' AND table_name='feat_returns' AND column_name='vol_60d') THEN
    ALTER TABLE feat.feat_returns ADD COLUMN vol_60d NUMERIC;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='feat' AND table_name='feat_returns' AND column_name='vol_spike_ratio') THEN
    ALTER TABLE feat.feat_returns ADD COLUMN vol_spike_ratio NUMERIC;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='feat' AND table_name='feat_returns' AND column_name='drawdown_52w') THEN
    ALTER TABLE feat.feat_returns ADD COLUMN drawdown_52w NUMERIC;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='feat' AND table_name='feat_returns' AND column_name='what_changed_score') THEN
    ALTER TABLE feat.feat_returns ADD COLUMN what_changed_score NUMERIC;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='core' AND table_name='core_events_earnings' AND column_name='expected_move') THEN
    ALTER TABLE core.core_events_earnings ADD COLUMN expected_move NUMERIC;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='core' AND table_name='core_events_earnings' AND column_name='reported_rev') THEN
    ALTER TABLE core.core_events_earnings ADD COLUMN reported_rev BIGINT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='core' AND table_name='core_events_earnings' AND column_name='guide_rev') THEN
    ALTER TABLE core.core_events_earnings ADD COLUMN guide_rev BIGINT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='core' AND table_name='core_events_earnings' AND column_name='post_notes') THEN
    ALTER TABLE core.core_events_earnings ADD COLUMN post_notes TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='core' AND table_name='core_events_earnings' AND column_name='thesis_impact') THEN
    ALTER TABLE core.core_events_earnings ADD COLUMN thesis_impact TEXT;
  END IF;
END $$;
