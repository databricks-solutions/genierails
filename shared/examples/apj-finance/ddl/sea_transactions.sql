-- Southeast Asian financial services — Singapore & Malaysia
-- Example DDL for testing GenieRails with country = "SEA"

CREATE TABLE apac_prod.sea_banking.customers (
    customer_id         BIGINT        COMMENT 'Unique customer identifier',
    full_name           STRING        COMMENT 'Customer full name',
    email               STRING        COMMENT 'Customer email address',
    phone               STRING        COMMENT 'Customer phone number',
    date_of_birth       DATE          COMMENT 'Date of birth',
    -- Singapore-specific
    nric                STRING        COMMENT 'Singapore NRIC or FIN',
    cpf_account         STRING        COMMENT 'CPF account number (same as NRIC)',
    uen                 STRING        COMMENT 'Company UEN (business customers)',
    -- Malaysia-specific
    mykad               STRING        COMMENT 'Malaysian MyKad IC number',
    epf_number          STRING        COMMENT 'EPF/KWSP member number',
    tin                 STRING        COMMENT 'Malaysian tax identification number',
    -- Common
    account_number      STRING        COMMENT 'Bank account number',
    address             STRING        COMMENT 'Street address',
    country_code        STRING        COMMENT 'SG or MY',
    customer_segment    STRING        COMMENT 'RETAIL, PREMIER, PRIVATE, SME',
    kyc_status          STRING        COMMENT 'VERIFIED, PENDING, EXPIRED',
    risk_rating         STRING        COMMENT 'LOW, MEDIUM, HIGH'
);

CREATE TABLE apac_prod.sea_banking.transactions (
    transaction_id      BIGINT        COMMENT 'Unique transaction identifier',
    customer_id         BIGINT        COMMENT 'FK to customers',
    transaction_date    TIMESTAMP     COMMENT 'Transaction timestamp',
    amount              DECIMAL(18,2) COMMENT 'Transaction amount',
    currency            STRING        COMMENT 'SGD or MYR',
    counterparty_name   STRING        COMMENT 'Counterparty or merchant name',
    counterparty_nric   STRING        COMMENT 'Counterparty NRIC (P2P transfers)',
    transaction_type    STRING        COMMENT 'PAYNOW, DUITNOW, GIRO, CARD, TRANSFER',
    status              STRING        COMMENT 'COMPLETED, PENDING, FAILED',
    channel             STRING        COMMENT 'MOBILE, ONLINE, BRANCH, ATM'
);
