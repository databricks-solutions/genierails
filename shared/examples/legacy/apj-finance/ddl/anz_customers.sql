-- Trans-Tasman banking customers — Australia & New Zealand
-- Example DDL for testing GenieRails with country = "ANZ"

CREATE TABLE apac_prod.banking.customers (
    customer_id         BIGINT        COMMENT 'Unique customer identifier',
    first_name          STRING        COMMENT 'Customer first name',
    last_name           STRING        COMMENT 'Customer last name',
    email               STRING        COMMENT 'Customer email address',
    phone               STRING        COMMENT 'Customer phone number',
    date_of_birth       DATE          COMMENT 'Date of birth',
    -- Australia-specific
    tax_file_number     STRING        COMMENT 'Australian Tax File Number (TFN)',
    medicare_number     STRING        COMMENT 'Australian Medicare card number',
    bsb                 STRING        COMMENT 'Bank State Branch number',
    -- New Zealand-specific
    ird_number          STRING        COMMENT 'NZ Inland Revenue Department number',
    nhi_number          STRING        COMMENT 'NZ National Health Index number',
    -- Common
    account_number      STRING        COMMENT 'Bank account number',
    driver_licence      STRING        COMMENT 'Driver licence number (AU or NZ)',
    address             STRING        COMMENT 'Street address',
    city                STRING        COMMENT 'City',
    state_province      STRING        COMMENT 'State (AU) or region (NZ)',
    postcode            STRING        COMMENT 'Postcode',
    country_code        STRING        COMMENT 'AU or NZ',
    customer_since      DATE          COMMENT 'Date customer was onboarded',
    risk_score          DOUBLE        COMMENT 'Internal risk assessment score',
    aml_flag            BOOLEAN       COMMENT 'AML/CTF flagged for review'
);

CREATE TABLE apac_prod.banking.transactions (
    transaction_id      BIGINT        COMMENT 'Unique transaction identifier',
    customer_id         BIGINT        COMMENT 'FK to customers',
    transaction_date    TIMESTAMP     COMMENT 'Transaction timestamp',
    amount              DECIMAL(18,2) COMMENT 'Transaction amount in local currency',
    currency            STRING        COMMENT 'AUD or NZD',
    merchant_name       STRING        COMMENT 'Merchant or payee name',
    merchant_category   STRING        COMMENT 'MCC category',
    bsb_dest            STRING        COMMENT 'Destination BSB (AU domestic transfers)',
    account_dest        STRING        COMMENT 'Destination account number',
    transaction_type    STRING        COMMENT 'DEBIT, CREDIT, TRANSFER, BPAY',
    status              STRING        COMMENT 'COMPLETED, PENDING, FAILED, REVERSED',
    aml_alert           BOOLEAN       COMMENT 'Triggered AML monitoring alert'
);
