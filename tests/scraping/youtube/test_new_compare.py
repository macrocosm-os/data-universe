from scraping.youtube.utils import texts_are_similar, compare_text_similarity

# Generated text samples (~50 words each, High Similarity >90%, Low Similarity ~60-70%)
# Japanese: High Similarity (>90%)
japanese_text1_high = "こんにちは世界日本の文化が大好きです音楽とアニメは素晴らしい伝統的な祭りと現代技術が融合した国食べ物は美味しく特に寿司とラーメンが好きです日本の歴史も魅力的で寺院や神社を訪れるのが楽しいです自然も美しく富士山は必見です日本の文化は特別です"
japanese_text2_high = "こんにちは世界日本の文化が大好きです音楽とアニメは素晴らしい伝統的な祭りと現代技術が融合した国食べ物は美味しく特に寿司とラーメンが好きです日本の歴史も魅力的で寺院や神社を訪れるのが楽しいです自然も美しく富士山は必見です日本の文化は最高です"

# Japanese: Low Similarity (~60-70%)
japanese_text1_low = "こんにちは世界日本の文化が大好きです音楽とアニメは素晴らしい伝統的な祭りと現代技術が融合した国食べ物は美味しく特に寿司とラーメンが好きです日本の歴史も魅力的で寺院や神社を訪れるのが楽しいです自然も美しく富士山は必見です日本の文化は特別です"
japanese_text2_low = "こんにちは日本日本の音楽が素晴らしいアニメと祭りが大好きです食べ物は寿司とラーメンが美味しい技術と文化が融合した国です歴史は魅力的で神社や寺院を訪れるのが楽しい富士山は美しく自然も素晴らしい日本の伝統は特別です日本の美しさは最高です"

# Arabic: High Similarity (>90%)
arabic_text1_high = "مرحبا بالعالم أحب الثقافة العربية الموسيقى والرقص التقليدي رائعان الأطعمة لذيذة مثل الحمص والفلافل والشاورما التاريخ العربي غني بالشعر والفن المعماري الإسلامي المذهل المساجد والأسواق التقليدية ممتعة للزيارة الصحراء والجبال تضيف جمالا طبيعيا فريدا"
arabic_text2_high = "مرحبا بالعالم أحب الثقافة العربية الموسيقى والرقص التقليدي رائعان الأطعمة لذيذة مثل الحمص والفلافل والشاورما التاريخ العربي غني بالشعر والفن المعماري الإسلامي المذهل المساجد والأسواق التقليدية ممتعة للزيارة الصحراء والجبال تضيف جمالا طبيعيا رائعا"

# Arabic: Low Similarity (~60-70%)
arabic_text1_low = "مرحبا بالعالم أحب الثقافة العربية الموسيقى والرقص التقليدي رائعان الأطعمة لذيذة مثل الحمص والفلافل والشاورما التاريخ العربي غني بالشعر والفن المعماري الإسلامي المذهل المساجد والأسواق التقليدية ممتعة للزيارة الصحراء والجبال تضيف جمالا طبيعيا فريدا"
arabic_text2_low = "مرحبا أحب الثقافة العربية الموسيقى رائعة والأطعمة مثل الحمص والشاورما لذيذة التاريخ غني بالشعر والفن المعماري الإسلامي ممتع المساجد والأسواق التقليدية رائعة للزيارة الصحراء جميلة والجبال طبيعية الثقافة العربية تضيف جمالا فريدا ومميزا للعالم"

# French (Diacritics): High Similarity (>90%)
french_text1_high = "Bonjour le monde, j’aime la culture française, la musique et la cuisine sont délicieuses, surtout les croissants et le fromage. L’histoire de France est riche, avec des châteaux et des musées magnifiques. Paris est une ville splendide, et la campagne offre des paysages pittoresques."
french_text2_high = "Bonjour le monde, j’aime la culture francaise, la musique et la cuisine sont delicieuses, surtout les croissants et le fromage. L’histoire de France est riche, avec des chateaux et des musees magnifiques. Paris est une ville splendide, et la campagne offre des paysages pittoresques."

# French (Diacritics): Low Similarity (~60-70%)
french_text1_low = "Bonjour le monde, j’aime la culture française, la musique et la cuisine sont délicieuses, surtout les croissants et le fromage. L’histoire de France est riche, avec des châteaux et des musées magnifiques. Paris est une ville splendide, et la campagne offre des paysages pittoresques."
french_text2_low = "Salut le monde, la culture française est belle, la musique est agréable et la cuisine inclut des croissants et du fromage. L’histoire est intéressante, avec des musées et quelques châteaux. Paris est une ville magnifique, la campagne a des paysages jolis et uniques."

# English: High Similarity (>90%)
english_text1_high = "Hello world, I love British culture, the music and food are amazing, especially fish and chips and tea. The history is rich with castles and museums. London is a vibrant city, and the countryside offers stunning landscapes. Literature and theater are truly remarkable."
english_text2_high = "Hello world, I love British culture, the music and food are amazing, especially fish and chips and tea. The history is rich with castles and museums. London is a vibrant city, and the countryside offers stunning landscapes. Literature and theater are truly wonderful."

# English: Low Similarity (~60-70%)
english_text1_low = "Hello world, I love British culture, the music and food are amazing, especially fish and chips and tea. The history is rich with castles and museums. London is a vibrant city, and the countryside offers stunning landscapes. Literature and theater are truly remarkable."
english_text2_low = "Hi world, British culture is great, the music is fantastic, and food like fish and chips is good. The history includes museums and old buildings. London is a lively city, the countryside is beautiful. Books and plays are quite interesting."

# Store text pairs for comparison
text_pairs = [
    ("Japanese (High Similarity)", japanese_text1_high, japanese_text2_high),
    ("Japanese (Low Similarity)", japanese_text1_low, japanese_text2_low),
    ("Arabic (High Similarity)", arabic_text1_high, arabic_text2_high),
    ("Arabic (Low Similarity)", arabic_text1_low, arabic_text2_low),
    ("French (Diacritics, High Similarity)", french_text1_high, french_text2_high),
    ("French (Diacritics, Low Similarity)", french_text1_low, french_text2_low),
    ("English (High Similarity)", english_text1_high, english_text2_high),
    ("English (Low Similarity)", english_text1_low, english_text2_low)
]

# Compare the two functions
for lang, text1, text2 in text_pairs:
    print(f"\n=== Comparing {lang} Texts ===")
    print(f"Text 1 (first 50 chars): {text1[:50]}...")
    print(f"Text 2 (first 50 chars): {text2[:50]}...")

    print("\nOriginal function (texts_are_similar):")
    result_original = texts_are_similar(text1, text2)
    print(f"Boolean result: {result_original}")

    print("\nImproved function (compare_text_similarity):")
    result_improved = compare_text_similarity(text1, text2)
    print(f"Boolean result: {result_improved}")
    print("=" * 50)
