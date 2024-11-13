# Friend Recommendation Using MapReduce and Apache Spark


Short Pseudocode:
----------------------
1. Input: Process data as (user, [friends_list]).

2. Map Step: For each user, generate friendships:
 
    - Direct: ((user1, user2), 0)
    - Mutual: For each pair of friends, ((min(friend1, friend2), 
    max(friend1, friend2)), 1)

3. Reduce Step: Sum values by key (user1, user2) to count mutual friends.

4. FlatMap: Convert ((user1, user2), mutual_count) into [(user1, (user2, mutual_count)), (user2, (user1, mutual_count))].

5. Group By: Group recommendations by user.

6. Sort and Truncate: For each user, sort friends by mutual_count (descending) and select the top 10.

7. Output: Format and print the final recommendations.



Algorithm
---------------------
1. Input Data Processing: we made entry have the index of the user and array of friends:
    ```python
    [(0, [1, 2, 3, 4, 5, ..., 94]),
    (1, [0, 5, 20, 135, 2409, ..., 49592]),
    (2, [0, 117, 135, 1220, ..., 41878]),
    (3, [0, 12, 41, 55, ..., 38737]),]
    ```
2. Map
- Direct friendships between user1 and user2 have value 0

    ```python
    ((0, 1), 0)
    ((0, 2), 0)
    ...
    ((0, 94), 0)
    ```
- Mutual friendships between user1 and user2 have value 1

    ```python
    ((1, 2), 1)
    ((1, 3), 1)
    ((1, 4), 1)
    ((2, 3), 1)
    ((2, 4), 1)
    ...
    ((93, 94), 1)
    ```
3. Reduce by Key: The key is the tuple of user1, user2. The value is the number of mutual 
friends.
- Example `((2, 3), 2)` means that `(2, 3)` have `2` mutual friends.
4. Process Data
- We do flatmap to change `((2, 3), 2)` to `[(2, (3, 2), 3, (2, 2))]` which is `2` is friends with `3` with `2` mutual friends and `3` is friends with `2` with `2` mutual friends.
- We do groupby key to get to `[(2, [(3, 2), (4, 5)]` so we have user1 and then all the people user1 
is friends with and how many mutual friends they have.
