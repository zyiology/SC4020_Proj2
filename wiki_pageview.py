from collections import defaultdict

# Create a dictionary to store the total pageviews for each article
article_pageviews = defaultdict(int)

id_name = {}

with open("data/pageviews-20231105-user", "r", encoding="utf-8") as file:
    i=0
    for line in file:
        # print(line)
        # i+=1
        # if i==10: break
        # Split the line into its components
        wiki_code, article_title, page_id, _, daily_total, _ = line.strip().split(" ")

        # Convert daily_total to an integer
        daily_total = int(daily_total)

        # Add the daily_total to the corresponding article in the dictionary
        article_pageviews[page_id] += daily_total

        id_name[page_id] = article_title

# Sort the articles by total pageviews in descending order
sorted_articles = sorted(article_pageviews.items(), key=lambda x: x[1], reverse=True)

# Calculate the total number of articles
total_articles = len(sorted_articles)

# Calculate the number of articles in the top 10%
top_10_percent = int(0.1 * total_articles)

# Extract the article IDs for the top 10% most popular articles
top_10_percent_articles = [article_id for article_id, pageviews in sorted_articles[:top_10_percent]]

# Print the article IDs for the top 10% most popular articles
#print(len(top_10_percent_articles))

for id in top_10_percent_articles[:50]:
    print(id_name[id])
