import typing
from apache_beam.utils.timestamp import Timestamp
from datetime import datetime, date, time, timezone
from apache_beam.utils.timestamp import Timestamp as ts

GradeRevenuesRow = typing.NamedTuple(
    'GradeRevenuesRow', [
        ('id', int),
        ('acc_uuid', str),
        ('cus_uuid', str),
        ('citizen_iden_uuid', str),
        ('tm_key_mth', str),
        ('identifier_id', str),
        ('identifier_name', str),
        ('identifier_desc', str),
        ('identifier_status', str),
        ('bill_status', str),
        ('identifier_start_date', Timestamp),
        ('identifier_aging', int),
        ('amount', int),
        ('tax_amount', int),
        ('rev_amount', int),
        ('avg_amount', int),
        ('operator_name', str),
        ('promoted_flag', str),
        ('audit_cu', int),
        ('audit_cd', Timestamp),
        ('audit_mu', int),
        ('audit_md', Timestamp)
    ]
)


def convert_to_utc_timestamp(value):
    """Converts datetime objects to Apache Beam's Timestamp."""
    if isinstance(value, datetime):
        return ts(micros=int(value.timestamp() * 1_000_000))
    elif isinstance(value, date):
        dt_value = datetime.combine(value, time.min, tzinfo=timezone.utc)
        return ts(micros=int(dt_value.timestamp() * 1_000_000))
    else:
        raise ValueError(f"Unsupported type for conversion: {type(value)}")
    
    
def dict_to_grade_revenues_row(row_dict, GradeRevenuesRow):
    return GradeRevenuesRow(
        id=row_dict['id'],
        acc_uuid=row_dict['acc_uuid'],
        cus_uuid=row_dict['cus_uuid'],
        citizen_iden_uuid=row_dict['citizen_iden_uuid'],
        tm_key_mth=row_dict['tm_key_mth'],
        identifier_id=row_dict['identifier_id'],
        identifier_name=row_dict['identifier_name'],
        identifier_desc=row_dict['identifier_desc'],
        identifier_status=row_dict['identifier_status'],
        bill_status=row_dict['bill_status'],
        identifier_start_date=convert_to_utc_timestamp(row_dict['identifier_start_date']),
        identifier_aging=row_dict['identifier_aging'],
        amount=row_dict['amount'],
        tax_amount=row_dict['tax_amount'],
        rev_amount=row_dict['rev_amount'],
        avg_amount=row_dict['avg_amount'],
        operator_name=row_dict['operator_name'],
        promoted_flag=row_dict['promoted_flag'],
        audit_cu=row_dict['audit_cu'],
        audit_cd=convert_to_utc_timestamp(row_dict['audit_cd']),
        audit_mu=row_dict['audit_mu'],
        audit_md=convert_to_utc_timestamp(row_dict['audit_md'])
    )
