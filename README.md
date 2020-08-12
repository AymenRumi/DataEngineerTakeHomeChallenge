# Mistplay Data Engineer Take Home Challenge


## Task Description

You will be required to produce code to process and transform some sample data.
The sample data is in the file called `data.csv`.
Certain rows contain missing entries which you will need to handle appropriately.
There are also some duplicate rows.

The produced code should be able to acheive the following
1. compute the rank of each user's `user_score` within each age group and output the rank in a new column called `sub_group_rank`
2. process the column `widget_list` (list of JSON) by
    1. flattening the list items i.e. each item in the list is put into its own row
    2. extracting the values in the JSON elements into their own columns called `widget_name` and `widget_amount`
3. remove duplicates over the columns `id` and `createdAt`
4. anonymize the column `email` and output the anonymized version in a new column `email_anon`.
This column `email_anon` should have the following properties.
    1. machine learning training and inference can be done on the anonyized values
    2. given an anonymized value, the original value can be recovered
5. 
6. write the processed data into separate csv file(s)

You will be evaluated on correctness, scalability and maintainability of your code.

## Guidelines

1. You are allowed to use any language and any libraries you wish.
However, you should be able to justify your technical decisions.
2. Fork the github repo [ here ](https://github.com/Mistplay/DataEngineerTakeHomeChallenge).
3. The challenge should not require more than a couple of hours to complete.
We don't want you to be spending too much time on it.
This being said, your code should be organized and well-designed within reason.
