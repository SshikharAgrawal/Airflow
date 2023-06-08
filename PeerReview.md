# Peer Review

## Kushagra Singh

### Task 1
In this task, he successfully created a DAG named **dag1** that runs on a daily schedule. The DAG contains three operations: **create_table**, **insert_data**, and **select_data**. These operations effectively handle the creation of a table named **users** if it doesn't already exist, inserting data into the table, and selecting data from the created table. 

### Task 2
For this task, he created a DAG called **dummy_dag** that is also scheduled daily. Within this DAG, he implemented two operations: **dummy_task** and **send_mail**. The **dummy_task** has been appropriately designed to always pass, indicating its successful execution. Additionally, the **send_mail** operation has been included to notify the developer of the task's successful completion

### Task 3
The **slack_alert_dag** he created is scheduled daily and consists of three operations: **dummy_task**, **success_failure_task**, and **slack_alert_task**. The **dummy_task** has been implemented to consistently pass, while the **success_failure_task** serves as a simulated task that can either pass or fail. Finally, the **slack_alert_task** has been incorporated to send a message to Slack, providing information about the outcome of the **success_failure_task**. 

## Shishir Singh

### Task 1
The DAG handles the creation of a PostgreSQL table named **users** and inserts sample data into it. The inclusion of the **create_table** task ensures that the table is only created if it doesn't already exist, which is a good practice. The **insert_data** task populates the table with sample data, and the **select_data** task retrieves all the data from the **users** table. 

### Task 2
He create a DAG that consists of two tasks: **dummy_task** and **email_task**. The main task, **email_task**, sends an email with a subject and content to a specified recipient.

### Task 3
The DAG consists of three tasks: **dummy_task**, **success_task** and **fail_task**. The **success_task** appropriately sends a success message to the Slack channel if all tasks succeed, while the **fail_task** sends a failure message if any task fails.
