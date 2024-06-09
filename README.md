# Personal Finance Tracker App

> Reload data 

    Run `tracker.py`

>  Data Visualization

    - Login to [AWS Quicksight dashboard](https://aws.amazon.com/QuickSight/)
    - Refresh dataset or schedule automatic refreshes

> [!NOTE]
> `manifest.json` is used to connect Quicksight to AWS resources


## Data Engineering
In data engineering, handling exceptions and errors is crucial to ensure the quality and reliability of your data pipelines. Here are some best practices:

1. Validate Input Data: Always validate the input data before processing it. This could involve checking for the presence of required columns, ensuring the correct data types, and verifying that the data falls within expected ranges.

2. Use Try/Except Blocks: Enclose the code that might raise an exception in a try/except block. This allows you to catch exceptions and handle them gracefully, rather than letting the entire script fail.

3. Log Errors: Use logging to record errors and exceptions. This can help you debug and troubleshoot issues. Include as much context as possible in your log messages, such as the data being processed when the error occurred.

4. Raise Meaningful Exceptions: When you raise exceptions, include a meaningful error message that explains what went wrong. This can help you understand the issue when you see the exception later.

5. Handle Specific Exceptions: Instead of catching all exceptions, catch specific exceptions that you expect might occur and know how to handle. This can help prevent silent failures.

6. Use Assertions: Use assertions to check for conditions that should always be true. This can help catch bugs or unexpected conditions.

7. Test Your Code: Write unit tests and integration tests for your code. This can help ensure that your code works correctly and continues to work correctly as you make changes.

8. Monitor Your Pipelines: Monitor your data pipelines to detect issues quickly. This could involve setting up alerts for when pipelines fail, or tracking metrics about data quality.

