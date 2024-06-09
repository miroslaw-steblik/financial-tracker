from banking.steblik_finance import barclays
from banking.keywords import keyword_groups


def test_barclays_no_duplicates():
    # Call the barclays function to get the DataFrame
    bdf = barclays()

    # Check for duplicates
    duplicates = bdf.duplicated(keep=False)

    # Assert that there are no duplicates
    assert not duplicates.any(), "Found duplicate transactions"

# Call the test function
test_barclays_no_duplicates()