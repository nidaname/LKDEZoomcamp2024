if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    data = data[(data['passenger_count'] != 0)  & (data['trip_distance'] != 0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    column_rename = {
        'VendorID': 'vendor_id',
        'RatecodeID': 'rate_code_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id'
    }
    # Specify your transformation logic here
    data = data.rename(columns=column_rename)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert set(1, 2) == set(output['vendor_id']), 'Vendor ID has other values besides 1, 2'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert min(output['passenger_count']) > 0, 'min passenger count <= 0'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert min(output['trip_distance']) > 0, 'min trip distance <= 0'