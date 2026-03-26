-- Indian financial services
-- Example DDL for testing GenieRails with country = "IN"

CREATE TABLE apac_prod.india_banking.customers (
    customer_id             BIGINT        COMMENT 'Unique customer identifier',
    first_name              STRING        COMMENT 'Customer first name',
    last_name               STRING        COMMENT 'Customer last name',
    email                   STRING        COMMENT 'Customer email address',
    mobile                  STRING        COMMENT 'Indian mobile number (+91...)',
    date_of_birth           DATE          COMMENT 'Date of birth',
    -- India-specific identifiers
    aadhaar_number          STRING        COMMENT 'Aadhaar UID (12 digits)',
    pan_number              STRING        COMMENT 'Permanent Account Number',
    voter_id                STRING        COMMENT 'Voter ID / EPIC number',
    driving_licence         STRING        COMMENT 'State driving licence number',
    -- Business / financial
    gstin                   STRING        COMMENT 'GST Identification Number',
    ifsc_code               STRING        COMMENT 'Bank IFSC code',
    account_number          STRING        COMMENT 'Bank account number',
    upi_id                  STRING        COMMENT 'UPI Virtual Payment Address',
    -- Common
    address                 STRING        COMMENT 'Address line',
    city                    STRING        COMMENT 'City',
    state                   STRING        COMMENT 'Indian state',
    pincode                 STRING        COMMENT 'PIN code',
    customer_type           STRING        COMMENT 'INDIVIDUAL, HUF, COMPANY, TRUST',
    kyc_status              STRING        COMMENT 'CKYC_VERIFIED, PENDING, INCOMPLETE',
    account_opened_date     DATE          COMMENT 'Date account was opened',
    credit_score            INT           COMMENT 'CIBIL credit score'
);

CREATE TABLE apac_prod.india_banking.transactions (
    transaction_id          BIGINT        COMMENT 'Unique transaction identifier',
    customer_id             BIGINT        COMMENT 'FK to customers',
    transaction_date        TIMESTAMP     COMMENT 'Transaction timestamp',
    amount                  DECIMAL(18,2) COMMENT 'Transaction amount in INR',
    transaction_type        STRING        COMMENT 'UPI, NEFT, RTGS, IMPS, CARD, CASH',
    upi_id_sender           STRING        COMMENT 'Sender UPI ID (for UPI txns)',
    upi_id_receiver         STRING        COMMENT 'Receiver UPI ID (for UPI txns)',
    ifsc_dest               STRING        COMMENT 'Destination bank IFSC',
    account_dest            STRING        COMMENT 'Destination account number',
    status                  STRING        COMMENT 'SUCCESS, PENDING, FAILED, REVERSED',
    gst_amount              DECIMAL(18,2) COMMENT 'GST component if applicable',
    tds_deducted            DECIMAL(18,2) COMMENT 'TDS deducted at source'
);
