import requests
from bs4 import BeautifulSoup


def download_books():
    # Base URL for Project Gutenberg's top books
    download_link = "http://gutenberg.org/cache/epub/"
    # List of book IDs from Project Gutenberg (replace with desired IDs)


    # book_ids = [1342, 11, 84, 1080, 98]  # Example IDs for Pride and Prejudice, Alice's Adventures in Wonderland, etc.
    book_ids = get_top_100_book_ids()
    for i, book_id in enumerate(book_ids):
        # Fetch the book's HTML page
        # Find the link to the plain text UTF-8 version
        text_res = requests.get(f"{download_link}{book_id}/pg{book_id}.txt")
        with open(f"../nfs/nfs-storage/input/book-{i}", 'w', encoding='utf-8') as file:
            file.write(text_res.text)

    print("Downloads complete.")

def get_top_100_book_ids():
    url = "http://gutenberg.org/browse/scores/top"
    res = requests.get(url)
        # Check if the request was successful
    if res.status_code != 200:
        print('Failed to retrieve the webpage')
        return []

    soup = BeautifulSoup(res.content, 'html.parser')
   
    # Finding the list of top 100 books. It's usually within 'ol' tags
    ol = soup.find_all('ol')

    # The first 'ol' tag contains top 100 ebooks IDs
    book_ids = []
    for li in ol[0].find_all('li'):
        book_link = li.a['href']
        # Extracting the book ID from the link
        book_id = book_link.split('/')[-1]
        book_ids.append(book_id)

    return book_ids
 
    

if __name__ == "__main__":
    download_books()