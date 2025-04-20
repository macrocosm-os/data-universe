import json
import re
import random
import time
from typing import List, Dict, Any


# Function to generate a synthetic transcript for a podcast
def generate_synthetic_transcript(duration_minutes: int, words_per_minute: int = 150) -> List[Dict[str, Any]]:
    """Generate a synthetic transcript for testing compression"""
    # Sample sentences to build our synthetic transcript
    sentences = [
        "Welcome to the podcast everyone.",
        "Today we're talking about artificial intelligence.",
        "The future of AI looks very promising.",
        "Machine learning models are getting more sophisticated.",
        "Neural networks can now generate realistic images and text.",
        "GPT models have revolutionized natural language processing.",
        "Many industries are being transformed by these technologies.",
        "Healthcare can benefit from AI diagnosis systems.",
        "Transportation will change with autonomous vehicles.",
        "Privacy concerns remain an important consideration.",
        "Regulation of AI is a complex topic.",
        "We need to ensure these systems are used ethically.",
        "The pace of innovation continues to accelerate.",
        "Quantum computing may further enhance AI capabilities.",
        "Let's discuss the implications for society.",
        "Jobs will be both created and eliminated by automation.",
        "Education systems need to adapt to these changes.",
        "Critical thinking will remain an essential human skill.",
        "Thank you for listening to our discussion today.",
        "Don't forget to subscribe for more content like this."
    ]

    # Calculate total words based on duration and speaking rate
    total_words = duration_minutes * words_per_minute

    # Generate transcript segments
    transcript = []
    current_time = 0.0
    words_so_far = 0

    while words_so_far < total_words:
        # Select a random sentence
        sentence = random.choice(sentences)
        word_count = len(sentence.split())

        # Calculate how long this sentence would take to say
        duration = word_count / (words_per_minute / 60)

        transcript.append({
            "text": sentence,
            "start": round(current_time, 2),
            "duration": round(duration, 2)
        })

        current_time += duration
        words_so_far += word_count

    return transcript


# Compression function
def compress_transcript(transcript: List[Dict[str, Any]], chunk_size: int = 3000) -> List[Dict[str, Any]]:
    """Compress the transcript by combining small segments into larger chunks"""
    if not transcript:
        return []

    # Extract the full text
    full_text = " ".join([item['text'] for item in transcript])

    # Remove redundant whitespace
    full_text = re.sub(r'\s+', ' ', full_text).strip()

    # Create chunks based on natural sentence boundaries
    chunks = []
    current_pos = 0

    while current_pos < len(full_text):
        # Find a good break point (end of sentence) within the chunk size
        end_pos = min(current_pos + chunk_size, len(full_text))

        # If we're not at the end, try to find a sentence break
        if end_pos < len(full_text):
            # Look for sentence endings (.!?) followed by space or end of text
            sentence_break = max(
                full_text.rfind('. ', current_pos, end_pos),
                full_text.rfind('! ', current_pos, end_pos),
                full_text.rfind('? ', current_pos, end_pos)
            )

            if sentence_break > current_pos:
                end_pos = sentence_break + 2  # Include the punctuation and space

        # Add the chunk
        chunks.append(full_text[current_pos:end_pos].strip())
        current_pos = end_pos

    # Create a compressed transcript with fewer entries
    compressed_transcript = []
    total_duration = sum(item.get('duration', 0) for item in transcript)

    if chunks:
        chunk_duration = total_duration / len(chunks)

        for i, chunk in enumerate(chunks):
            start_time = i * chunk_duration
            compressed_transcript.append({
                'text': chunk,
                'start': round(start_time, 2),
                'duration': round(chunk_duration, 2)
            })

    return compressed_transcript


# Function to analyze the compression
def analyze_compression(original: List[Dict[str, Any]], compressed: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze the compression results"""
    original_json = json.dumps(original)
    compressed_json = json.dumps(compressed)

    original_size = len(original_json)
    compressed_size = len(compressed_json)
    compression_ratio = original_size / compressed_size if compressed_size > 0 else 0

    original_words = sum(len(item['text'].split()) for item in original)
    compressed_words = sum(len(item['text'].split()) for item in compressed)

    return {
        "original_segments": len(original),
        "compressed_chunks": len(compressed),
        "original_size_bytes": original_size,
        "compressed_size_bytes": compressed_size,
        "compression_ratio": compression_ratio,
        "original_words": original_words,
        "compressed_words": compressed_words,
        "word_retention_ratio": compressed_words / original_words if original_words > 0 else 0
    }


# Demo with different podcast lengths
def run_demo():
    print("YouTube Transcript Compression Demo")
    print("==================================")

    # Test different podcast durations
    for duration in [30, 60, 120, 240]:  # minutes
        print(f"\nCompressing {duration}-minute podcast:")

        # Generate synthetic transcript
        original_transcript = generate_synthetic_transcript(duration)

        # Time the compression
        start_time = time.time()
        compressed_transcript = compress_transcript(original_transcript)
        compression_time = time.time() - start_time

        # Analyze results
        analysis = analyze_compression(original_transcript, compressed_transcript)

        # Print results]
        print(analysis['compressed_size_bytes'])
        print(f"  Original: {analysis['original_segments']} segments, {analysis['original_size_bytes']:,} bytes")
        print(f"  Compressed: {analysis['compressed_chunks']} chunks, {analysis['compressed_size_bytes']:,} bytes")
        print(f"  Compression ratio: {analysis['compression_ratio']:.2f}x")
        print(
            f"  Words: {analysis['original_words']} → {analysis['compressed_words']} ({analysis['word_retention_ratio']:.2%} retention)")
        print(f"  Compression time: {compression_time:.3f} seconds")

        # Show a sample of the compressed transcript (first chunk)
        if compressed_transcript:
            print("\n  Sample of first compressed chunk:")
            print(f"  Start: {compressed_transcript[0]['start']}s, Duration: {compressed_transcript[0]['duration']}s")
            print(f"  Text excerpt: {compressed_transcript[0]['text'][:100]}...")


if __name__ == "__main__":
    run_demo()