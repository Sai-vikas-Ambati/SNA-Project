import praw
import csv
import time
import logging
from datetime import datetime
import os
from collections import defaultdict, Counter
import pandas as pd
import networkx as nx

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MultiCommunityRedditScraper:
    def __init__(self, client_id, client_secret, user_agent):
        """
        Initialize Reddit scraper  for multi-community analysis
        """
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        
        # Track communities and users
        self.communities = set()
        self.user_communities = defaultdict(set)  # user -> set of communities
        self.community_users = defaultdict(set)   # community -> set of users
        self.cross_posts = []  # Track cross-posts between communities
        self.user_interactions = defaultdict(list)  # Track user reply networks
        
        # Enhanced CSV fieldnames for multi-community analysis
        self.post_fieldnames = [
            'post_id', 'title', 'author', 'author_fullname', 'subreddit',
            'created_utc', 'score', 'upvote_ratio', 'num_comments', 
            'selftext', 'url', 'permalink', 'link_karma', 'comment_karma',
            'is_original_content', 'stickied', 'locked', 'archived',
            'crosspost_parent', 'is_crosspost'  # New fields for cross-posting
        ]
        
        self.comment_fieldnames = [
            'comment_id', 'post_id', 'parent_id', 'author', 'author_fullname',
            'subreddit', 'body', 'score', 'created_utc', 'edited',
            'is_submitter', 'permalink', 'depth', 'link_karma', 'comment_karma',
            'parent_author'  # New field for interaction analysis
        ]
        
        # New CSV for interconnection analysis
        self.interconnection_fieldnames = [
            'user', 'community1', 'community2', 'interaction_type', 
            'interaction_count', 'first_interaction', 'last_interaction'
        ]
        
    def setup_csv_files(self, base_filename='reddit_multi_community'):
        """Setup CSV files for multi-community analysis"""
        self.posts_filename = f'{base_filename}_posts.csv'
        self.comments_filename = f'{base_filename}_comments.csv'
        self.interconnections_filename = f'{base_filename}_interconnections.csv'
        self.community_stats_filename = f'{base_filename}_community_stats.csv'
        
        # Create posts CSV
        with open(self.posts_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.post_fieldnames)
            writer.writeheader()
            
        # Create comments CSV  
        with open(self.comments_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.comment_fieldnames)
            writer.writeheader()
            
        # Create interconnections CSV
        with open(self.interconnections_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.interconnection_fieldnames)
            writer.writeheader()
            
        logger.info(f"Multi-community CSV files created")
        
    def get_user_metrics(self, author):
        """Safely get user karma metrics"""
        try:
            if author and hasattr(author, 'link_karma'):
                return author.link_karma, author.comment_karma
        except:
            pass
        return None, None
        
    def extract_post_data(self, post):
        """Extract post data with cross-posting detection"""
        link_karma, comment_karma = self.get_user_metrics(post.author)
        
        # Check if it's a crosspost
        is_crosspost = hasattr(post, 'crosspost_parent_list') and len(post.crosspost_parent_list) > 0
        crosspost_parent = None
        if is_crosspost:
            crosspost_parent = post.crosspost_parent_list[0] if post.crosspost_parent_list else None
        
        return {
            'post_id': post.id,
            'title': post.title,
            'author': str(post.author) if post.author else '[deleted]',
            'author_fullname': post.author_fullname if post.author else None,
            'subreddit': str(post.subreddit),
            'created_utc': int(post.created_utc),
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'selftext': post.selftext,
            'url': post.url,
            'permalink': post.permalink,
            'link_karma': link_karma,
            'comment_karma': comment_karma,
            'is_original_content': post.is_original_content,
            'stickied': post.stickied,
            'locked': post.locked,
            'archived': post.archived,
            'crosspost_parent': str(crosspost_parent) if crosspost_parent else None,
            'is_crosspost': is_crosspost
        }
        
    def extract_comment_data(self, comment, post_id):
        """Extract comment data with parent author tracking"""
        link_karma, comment_karma = self.get_user_metrics(comment.author)
        
        # Get parent comment author for interaction analysis
        parent_author = None
        try:
            if hasattr(comment, 'parent') and comment.parent():
                parent = comment.parent()
                if hasattr(parent, 'author') and parent.author:
                    parent_author = str(parent.author)
        except:
            pass
        
        return {
            'comment_id': comment.id,
            'post_id': post_id,
            'parent_id': comment.parent_id,
            'author': str(comment.author) if comment.author else '[deleted]',
            'author_fullname': comment.author_fullname if comment.author else None,
            'subreddit': str(comment.subreddit),
            'body': comment.body,
            'score': comment.score,
            'created_utc': int(comment.created_utc),
            'edited': comment.edited if comment.edited else False,
            'is_submitter': comment.is_submitter,
            'permalink': comment.permalink,
            'depth': comment.depth if hasattr(comment, 'depth') else 0,
            'link_karma': link_karma,
            'comment_karma': comment_karma,
            'parent_author': parent_author
        }
        
    def track_user_community_activity(self, author, subreddit, activity_type='post'):
        """Track which users are active in which communities"""
        if author and author != '[deleted]':
            self.communities.add(subreddit)
            self.user_communities[author].add(subreddit)
            self.community_users[subreddit].add(author)
            
    def track_user_interaction(self, comment_author, parent_author, subreddit, timestamp):
        """Track user-to-user interactions within communities"""
        if comment_author and parent_author and comment_author != '[deleted]' and parent_author != '[deleted]':
            self.user_interactions[comment_author].append({
                'target_user': parent_author,
                'community': subreddit,
                'timestamp': timestamp,
                'interaction_type': 'reply'
            })
            
    def write_post_to_csv(self, post_data):
        """Write post data and track community activity"""
        try:
            with open(self.posts_filename, 'a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.post_fieldnames)
                writer.writerow(post_data)
                
            # Track user-community relationships
            self.track_user_community_activity(
                post_data['author'], 
                post_data['subreddit'], 
                'post'
            )
            
            # Track cross-posts
            if post_data['is_crosspost']:
                self.cross_posts.append(post_data)
                
        except Exception as e:
            logger.error(f"Error writing post to CSV: {e}")
            
    def write_comment_to_csv(self, comment_data):
        """Write comment data and track interactions"""
        try:
            with open(self.comments_filename, 'a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.comment_fieldnames)
                writer.writerow(comment_data)
                
            # Track user-community relationships
            self.track_user_community_activity(
                comment_data['author'], 
                comment_data['subreddit'], 
                'comment'
            )
            
            # Track user interactions
            self.track_user_interaction(
                comment_data['author'],
                comment_data['parent_author'],
                comment_data['subreddit'],
                comment_data['created_utc']
            )
            
        except Exception as e:
            logger.error(f"Error writing comment to CSV: {e}")
            
    def scrape_multiple_communities(self, subreddit_list, posts_per_community=50, comments_per_post=30):
        """
        Scrape multiple communities for interconnection analysis
        
        Args:
            subreddit_list: List of subreddit names to scrape
            posts_per_community: Number of posts per subreddit
            comments_per_post: Max comments per post
        """
        logger.info(f"Starting multi-community scrape of {len(subreddit_list)} communities")
        
        total_communities = len(subreddit_list)
        
        for i, subreddit_name in enumerate(subreddit_list, 1):
            logger.info(f"Scraping community {i}/{total_communities}: r/{subreddit_name}")
            
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                posts = list(subreddit.hot(limit=posts_per_community))
                
                for j, post in enumerate(posts, 1):
                    try:
                        # Scrape post data
                        post_data = self.extract_post_data(post)
                        self.write_post_to_csv(post_data)
                        
                        # Scrape post comments
                        self.scrape_post_comments(post.id, comments_per_post)
                        
                        if j % 10 == 0:
                            logger.info(f"  Completed {j}/{len(posts)} posts in r/{subreddit_name}")
                        
                        # Rate limiting
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error processing post {post.id}: {e}")
                        continue
                        
                # Longer pause between communities
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"Error scraping r/{subreddit_name}: {e}")
                continue
                
        logger.info("Multi-community scraping completed!")
        self.analyze_interconnections()
        
    def scrape_post_comments(self, post_id, max_comments=100):
        """Scrape comments with interaction tracking"""
        try:
            post = self.reddit.submission(id=post_id)
            post.comments.replace_more(limit=0)
            
            comments_scraped = 0
            
            for comment in post.comments.list():
                if comments_scraped >= max_comments:
                    break
                    
                try:
                    if comment.author is None:
                        continue
                        
                    comment_data = self.extract_comment_data(comment, post_id)
                    self.write_comment_to_csv(comment_data)
                    comments_scraped += 1
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping comments for post {post_id}: {e}")
            
    def analyze_interconnections(self):
        """Analyze interconnections between communities"""
        logger.info("Analyzing community interconnections...")
        
        interconnection_data = []
        
        # Find users active in multiple communities
        multi_community_users = {
            user: communities for user, communities in self.user_communities.items() 
            if len(communities) > 1
        }
        
        logger.info(f"Found {len(multi_community_users)} users active in multiple communities")
        
        # Analyze cross-community connections
        for user, communities in multi_community_users.items():
            communities_list = list(communities)
            
            # Create pairs of communities this user connects
            for i in range(len(communities_list)):
                for j in range(i + 1, len(communities_list)):
                    community1 = communities_list[i]
                    community2 = communities_list[j]
                    
                    # Count user's activity in each community
                    user_interactions_c1 = len([
                        interaction for interaction in self.user_interactions.get(user, [])
                        if interaction['community'] == community1
                    ])
                    
                    user_interactions_c2 = len([
                        interaction for interaction in self.user_interactions.get(user, [])
                        if interaction['community'] == community2
                    ])
                    
                    interconnection_data.append({
                        'user': user,
                        'community1': community1,
                        'community2': community2,
                        'interaction_type': 'multi_community_user',
                        'interaction_count': user_interactions_c1 + user_interactions_c2,
                        'first_interaction': None,  # Could be calculated from timestamps
                        'last_interaction': None
                    })
        
        # Write interconnection data
        with open(self.interconnections_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.interconnection_fieldnames)
            writer.writeheader()
            for row in interconnection_data:
                writer.writerow(row)
                
        # Generate community statistics
        self.generate_community_stats()
        
        logger.info(f"Interconnection analysis complete. Found {len(interconnection_data)} connections")
        
    def generate_community_stats(self):
        """Generate statistics about each community and their interconnections"""
        stats_data = []
        
        for community in self.communities:
            users_in_community = self.community_users[community]
            
            # Count multi-community users
            multi_community_count = sum(
                1 for user in users_in_community 
                if len(self.user_communities[user]) > 1
            )
            
            # Calculate interconnection ratio
            interconnection_ratio = multi_community_count / len(users_in_community) if users_in_community else 0
            
            # Find connected communities
            connected_communities = set()
            for user in users_in_community:
                if len(self.user_communities[user]) > 1:
                    connected_communities.update(self.user_communities[user] - {community})
            
            stats_data.append({
                'community': community,
                'total_users': len(users_in_community),
                'multi_community_users': multi_community_count,
                'interconnection_ratio': round(interconnection_ratio, 3),
                'connected_communities_count': len(connected_communities),
                'connected_communities': ', '.join(sorted(connected_communities))
            })
        
        # Write community stats
        stats_fieldnames = [
            'community', 'total_users', 'multi_community_users', 
            'interconnection_ratio', 'connected_communities_count', 'connected_communities'
        ]
        
        with open(self.community_stats_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=stats_fieldnames)
            writer.writeheader()
            for row in stats_data:
                writer.writerow(row)
        
        logger.info("Community statistics generated")
        
    def print_analysis_summary(self):
        """Print summary of the multi-community analysis"""
        print(f"\n=== MULTI-COMMUNITY ANALYSIS SUMMARY ===")
        print(f"Total Communities Analyzed: {len(self.communities)}")
        print(f"Total Unique Users: {len(self.user_communities)}")
        
        multi_community_users = sum(1 for communities in self.user_communities.values() if len(communities) > 1)
        print(f"Multi-Community Users: {multi_community_users}")
        print(f"Cross-Posts Found: {len(self.cross_posts)}")
        
        print(f"\nCommunities: {', '.join(sorted(self.communities))}")
        
        print(f"\nFiles Generated:")
        print(f"- {self.posts_filename}")
        print(f"- {self.comments_filename}")
        print(f"- {self.interconnections_filename}")
        print(f"- {self.community_stats_filename}")


# Usage Example for Multi-Community Analysis
def main():
    # Reddit API credentials
    CLIENT_ID = "..."
    CLIENT_SECRET = "..."
    USER_AGENT = "..."

    
    # Initialize multi-community scraper
    scraper = MultiCommunityRedditScraper(CLIENT_ID, CLIENT_SECRET, USER_AGENT)
    
    # Setup CSV files
    scraper.setup_csv_files('multi_community_analysis')
    
    # Define multiple related communities for analysis
    # Example: Tech-related communities
    communities_to_analyze = [
        "MachineLearning",
        "artificial", 
        "datascience",
        "programming",
        "Python",
        "deeplearning"
    ]
    
    # Alternative community sets for different research focuses:
    
    # Gaming communities
    # communities_to_analyze = ["gaming", "pcgaming", "nintendo", "playstation", "xbox"]
    
    # Science communities  
    # communities_to_analyze = ["science", "physics", "chemistry", "biology", "askscience"]
    
    # Political communities (be careful with sensitive topics)
    # communities_to_analyze = ["politics", "politicaldiscussion", "neutralpolitics"]
    
    print(f"Analyzing interconnections between {len(communities_to_analyze)} communities:")
    for community in communities_to_analyze:
        print(f"- r/{community}")
    
    # Scrape multiple communities with interconnection analysis
    scraper.scrape_multiple_communities(
        subreddit_list=communities_to_analyze,
        posts_per_community=30,  # Adjust based on your needs
        comments_per_post=20     # Adjust based on your needs
    )
    
    # Print analysis summary
    scraper.print_analysis_summary()
    
    print("\nMulti-community scraping completed!")
    print("Check the generated CSV files for detailed interconnection analysis.")

if __name__ == "__main__":
    main()