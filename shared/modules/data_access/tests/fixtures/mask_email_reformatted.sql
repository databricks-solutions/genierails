-- Same definitions as mask_email.sql, deliberately reordered and reformatted.
-- formatting should not deploy anything
CREATE OR REPLACE FUNCTION cat.sch.mask_email ( v STRING )
RETURNS STRING
RETURN '***' ;
