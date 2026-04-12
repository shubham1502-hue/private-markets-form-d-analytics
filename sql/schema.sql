-- ============================================================
-- schema.sql
-- Creates the form_d database and table in MySQL
-- Run this first before loading data
-- ============================================================

CREATE DATABASE IF NOT EXISTS form_d_analytics;
USE form_d_analytics;

DROP TABLE IF EXISTS form_d;

CREATE TABLE form_d (
    id                      INT AUTO_INCREMENT PRIMARY KEY,
    accession_number        VARCHAR(30),
    cik                     VARCHAR(15),
    issuer_name             VARCHAR(255)        NOT NULL,
    issuer_name_clean       VARCHAR(255),
    filing_date             DATE                NOT NULL,
    filing_year             SMALLINT,
    filing_quarter          VARCHAR(8),
    filing_month            VARCHAR(8),
    form_type               VARCHAR(5),
    fund_type               VARCHAR(60)         NOT NULL,
    asset_class_group       VARCHAR(60)         NOT NULL,
    federal_exemption       VARCHAR(20),
    state_of_incorporation  VARCHAR(10),
    total_offering_amount   DECIMAL(20, 2),
    amount_sold             DECIMAL(20, 2),
    pct_capital_raised      DECIMAL(6, 4),
    num_investors           INT,
    investor_bucket         VARCHAR(30),
    fund_size_bucket        VARCHAR(30),
    quality_tier            VARCHAR(20),
    is_duplicate            TINYINT(1)          DEFAULT 0,
    is_outlier_offering     TINYINT(1)          DEFAULT 0,

    INDEX idx_filing_date       (filing_date),
    INDEX idx_filing_quarter    (filing_quarter),
    INDEX idx_filing_year       (filing_year),
    INDEX idx_fund_type         (fund_type),
    INDEX idx_asset_class       (asset_class_group),
    INDEX idx_exemption         (federal_exemption),
    INDEX idx_quality_tier      (quality_tier),
    INDEX idx_fund_size_bucket  (fund_size_bucket)
);
