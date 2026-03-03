import os
import glob

dir_path = 'c:/Users/HP/OneDrive/Desktop/PROJECTS/Segmento-Assignment/ui/templates/connectors'
files = glob.glob(os.path.join(dir_path, '*.html'))

insert_html = """
    <!-- Back Button -->
    <div class="mb-6" data-aos="fade-right">
      <a href="javascript:history.back()"
        class="inline-flex items-center gap-2 px-5 py-2.5 rounded-full border border-cyan-400 bg-cyan-950/50 text-cyan-400 text-sm font-bold hover:bg-cyan-400 hover:text-slate-900 transition-all shadow-[0_0_15px_rgba(34,211,238,0.2)] hover:shadow-[0_0_25px_rgba(34,211,238,0.5)] hover:-translate-x-1">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M10 19l-7-7m0 0l7-7m-7 7h18">
          </path>
        </svg>
        Back to previous
      </a>
    </div>
"""

count = 0
for file_path in files:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    search_str = '<div class="max-w-7xl mx-auto relative z-10 space-y-24">'
    if search_str in content and '<!-- Back Button -->' not in content:
        new_content = content.replace(search_str, search_str + insert_html)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        count += 1

print(f"Updated {count} files.")
