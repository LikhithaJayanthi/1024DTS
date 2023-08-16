from mrjob.job import MRJob
import csv

class YelpMapReduce(MRJob):

    """ReviewCountPerStateJob : This MapReduce job processes a Yelp business dataset to calculate the 
    total review count per state for businesses that are marked as open
    
    Author : Mahmood Hossain
    ID : c0896079"""
    
    def mapper(self, _, line):
        # Parse the CSV line
        row = next(csv.reader([line]))
        
        # Skip the header row
        if row[0] != '_c0':
            state = row[5]  
            
            # Handling non-numeric values in review_count column
            try:
                review_count = int(row[10])  
            except ValueError:
                review_count = 0  # Set to 0 if it's not a valid integer
            
            # Handling non-numeric values in is_open column
            try:
                is_open = int(row[11])  
            except ValueError:
                is_open = 0  # Set to 0 if it's not a valid integer
            
            # Apply a filtering condition (example: only process open businesses)
            if is_open == 1:
                # Emit state and review_count
                yield state, review_count
    
    def reducer(self, state, review_counts):
        # Reduce operation: Calculate the total review count per state
        total_reviews = sum(review_counts)
        
        # Emit state and total_reviews
        yield state, total_reviews

if __name__ == '__main__':
    YelpMapReduce.run()
