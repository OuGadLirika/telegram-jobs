import json
import os
from bs4 import BeautifulSoup
import textwrap

JOBS_FILE = "jobs.json"
LINKS_FILE = "links.json"

def load_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    return {}

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ")

def summarize_description(text, max_sentences=2):
    sentences = [s.strip() for s in text.replace("\n", " ").split('.') if s.strip()]
    return '. '.join(sentences[:max_sentences]) + '.' if sentences else ""

def format_post(job, link):
    title = job.get("title", "Untitled")
    description_html = job.get("description", "")
    description_text = clean_html(description_html)
    short_desc = summarize_description(description_text)

    return textwrap.dedent(f"""
    🌍 {title} - Remote ✅

    {short_desc}

    👤 More details: [Apply now]({link})
    ➡️ Post your vacancy: @seevov
    """).strip()

def main():
    jobs = load_json(JOBS_FILE)
    links = load_json(LINKS_FILE)

    for job_id, job in list(jobs.items())[-5:]:  # последние 5 вакансий
        link = links.get(job_id, job.get("url"))
        post = format_post(job, link)
        print(post)
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    main()
