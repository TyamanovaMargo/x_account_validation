import os
import subprocess
import requests
import pandas as pd
from typing import List, Dict, Optional
from urllib.parse import urlparse
import time
import logging
import json
import re

class VoiceSampleExtractor:
    def __init__(self, output_dir="voice_samples", min_duration=30, max_duration=3600, quality="192"):
        self.output_dir = output_dir
        self.min_duration = min_duration  # Minimum 30 seconds
        self.max_duration = max_duration  # Maximum 1 hour (3600 seconds)
        self.quality = quality  # kbps
        os.makedirs(output_dir, exist_ok=True)
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        print(f"🎤 VoiceSampleExtractor initialized:")
        print(f"📁 Output directory: {output_dir}")
        print(f"⏱️  Duration range: {min_duration}s - {max_duration}s")
        print(f"🎵 Audio quality: {quality} kbps")

    def extract_voice_samples(self, confirmed_voice_links: List[Dict]) -> List[Dict]:
        """Extract voice samples with dynamic duration (30s to 1 hour)"""
        if not confirmed_voice_links:
            print("🔍 No confirmed voice links to extract samples from")
            return []

        print(f"🎤 Starting enhanced voice sample extraction for {len(confirmed_voice_links)} links...")
        print(f"📁 Samples will be saved to: {self.output_dir}")
        print(f"⏱️ Duration strategy: Extract maximum available (30s - 1 hour)")
        print(f"📝 Filename format: username_source_duration_timestamp.mp3")

        extracted_samples = []
        
        for i, link_data in enumerate(confirmed_voice_links, 1):
            url = link_data.get('url', '')
            username = self._extract_best_username(link_data, url)
            platform = link_data.get('platform_type', 'unknown')
            
            if not url:
                print(f" ⚠️ Skipping entry {i} - no URL provided")
                continue

            print(f"🎤 [{i}/{len(confirmed_voice_links)}] Processing @{username} ({platform})")
            
            # Get optimal duration for this content
            optimal_duration = self._get_optimal_duration(url, platform)
            
            # Generate filename with duration info
            safe_username = self._sanitize_filename(username)
            safe_platform = platform.lower() if platform else 'unknown'
            timestamp = int(time.time())
            filename = f"{safe_username}_{safe_platform}_{optimal_duration}s_{timestamp}"
            
            extraction_result = self._extract_audio_sample(url, filename, platform, safe_username, optimal_duration)
            
            # Add extraction results to link data
            link_data.update({
                'sample_extracted': extraction_result['success'],
                'sample_file': extraction_result.get('file_path'),
                'extraction_status': extraction_result['status'],
                'sample_duration': optimal_duration,
                'actual_duration': extraction_result.get('actual_duration', optimal_duration),
                'sample_quality': self.quality,
                'processed_username': safe_username,
                'sample_filename': filename + '.mp3',
                'platform_source': safe_platform,
                'original_username': username
            })

            if extraction_result['success']:
                extracted_samples.append(link_data)
                print(f" ✅ Sample saved: {filename}.mp3 ({optimal_duration}s)")
            else:
                print(f" ❌ Failed: {extraction_result['status']}")

            time.sleep(2)  # Rate limiting

        self._print_extraction_summary(extracted_samples, len(confirmed_voice_links))
        return extracted_samples

    def _get_optimal_duration(self, url: str, platform: str) -> int:
        """Determine optimal sample duration based on content length"""
        try:
            print(f" 🔍 Analyzing content duration...")
            
            # Get duration using yt-dlp
            cmd = ['yt-dlp', '--get-duration', '--quiet', '--no-warnings', url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0 and result.stdout.strip():
                duration_str = result.stdout.strip()
                total_seconds = self._parse_duration_string(duration_str)
                
                if total_seconds > 0:
                    # Apply min/max constraints
                    optimal = max(self.min_duration, min(total_seconds, self.max_duration))
                    
                    print(f" ⏱️ Content duration: {total_seconds}s → Using: {optimal}s")
                    
                    if total_seconds < self.min_duration:
                        print(f" ⚠️ Content too short ({total_seconds}s), using minimum {self.min_duration}s")
                    elif total_seconds > self.max_duration:
                        print(f" ✂️ Content too long ({total_seconds}s), capping at {self.max_duration}s")
                    
                    return optimal
            
            # Fallback to maximum duration
            print(f" ⚠️ Could not determine duration, using maximum {self.max_duration}s")
            return self.max_duration
            
        except Exception as e:
            print(f" ⚠️ Duration analysis failed: {str(e)[:50]}")
            return self.max_duration

    def _parse_duration_string(self, duration_str: str) -> int:
        """Parse duration string (HH:MM:SS or MM:SS) to seconds"""
        try:
            parts = duration_str.split(':')
            if len(parts) == 3:  # HH:MM:SS
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:  # MM:SS
                return int(parts) * 60 + int(parts[1])
            else:
                return int(float(parts))  # Single number in seconds
        except:
            return 0

    def _extract_audio_sample(self, url: str, filename: str, platform: str, nickname: str, duration: int) -> Dict:
        """Extract audio sample with specified duration"""
        output_path = os.path.join(self.output_dir, f"{filename}.mp3")
        
        try:
            if platform == 'youtube':
                return self._extract_youtube_sample(url, output_path, nickname, duration)
            elif platform == 'twitch':
                return self._extract_twitch_sample(url, output_path, nickname, duration)
            else:
                return {
                    'success': False,
                    'status': f'unsupported_platform: {platform}'
                }
        except Exception as e:
            return {
                'success': False,
                'status': f'extraction_error_for_{nickname}: {str(e)[:100]}'
            }

    def _extract_youtube_sample(self, url: str, output_path: str, nickname: str, duration: int) -> Dict:
        """Extract YouTube audio with dynamic duration and quality fallback"""
        
        # Calculate timeout based on duration (more time for longer samples)
        timeout_multiplier = max(1, duration // 300)  # Extra time for every 5 minutes
        
        quality_options = [
            (self.quality, 300 * timeout_multiplier),
            ("128", 240 * timeout_multiplier),
            ("96", 180 * timeout_multiplier),
            ("64", 120 * timeout_multiplier)
        ]
        
        for quality, timeout in quality_options:
            try:
                print(f" 🎧 Trying YouTube {quality} kbps ({duration}s, timeout: {timeout}s)")
                
                cmd = [
                    'yt-dlp',
                    '--extract-audio',
                    '--audio-format', 'mp3',
                    '--audio-quality', quality,
                    '--postprocessor-args', f'ffmpeg:-t {duration}',  # Dynamic duration
                    '--output', output_path.replace('.mp3', '.%(ext)s'),
                    '--no-playlist',
                    '--quiet',
                    '--no-warnings',
                    '--ignore-errors',
                    '--fragment-retries', '5',
                    '--retries', '5',
                    '--socket-timeout', '30',
                    url
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                
                if result.returncode == 0 and os.path.exists(output_path):
                    file_size = os.path.getsize(output_path)
                    return {
                        'success': True,
                        'file_path': output_path,
                        'status': f'youtube_success_{nickname}_quality_{quality}_duration_{duration}s',
                        'actual_duration': duration,
                        'file_size': file_size
                    }
                else:
                    print(f" ⚠️ Quality {quality} failed, trying next...")
                    
            except subprocess.TimeoutExpired:
                print(f" ⏰ Timeout at {quality} kbps ({timeout}s), trying lower quality...")
                continue
            except Exception as e:
                print(f" ❌ Error at {quality}: {str(e)[:50]}")
                continue
        
        return {
            'success': False,
            'status': f'youtube_failed_all_qualities_{nickname}_duration_{duration}s'
        }

    def _extract_twitch_sample(self, url: str, output_path: str, nickname: str, duration: int) -> Dict:
        """Extract Twitch audio with dynamic duration"""
        
        # Calculate timeout based on duration
        timeout_multiplier = max(1, duration // 300)
        
        quality_options = [
            (self.quality, 400 * timeout_multiplier),
            ("128", 350 * timeout_multiplier),
            ("96", 300 * timeout_multiplier),
            ("64", 250 * timeout_multiplier)
        ]
        
        # Handle different Twitch URL types
        if '/videos/' not in url and '/clip/' not in url:
            if not url.endswith('/videos'):
                videos_url = url.rstrip('/') + '/videos'
                return self._try_get_recent_twitch_vod(videos_url, output_path, nickname, duration)
        
        # Direct VOD or clip URL
        for quality, timeout in quality_options:
            try:
                print(f" 🎧 Trying Twitch {quality} kbps ({duration}s, timeout: {timeout}s)")
                
                cmd = [
                    'yt-dlp',
                    '--extract-audio',
                    '--audio-format', 'mp3',
                    '--audio-quality', quality,
                    '--postprocessor-args', f'ffmpeg:-t {duration}',  # Dynamic duration
                    '--output', output_path.replace('.mp3', '.%(ext)s'),
                    '--quiet',
                    '--no-warnings',
                    '--ignore-errors',
                    '--fragment-retries', '5',
                    '--retries', '5',
                    '--socket-timeout', '30',
                    url
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                
                if result.returncode == 0 and os.path.exists(output_path):
                    file_size = os.path.getsize(output_path)
                    return {
                        'success': True,
                        'file_path': output_path,
                        'status': f'twitch_success_{nickname}_quality_{quality}_duration_{duration}s',
                        'actual_duration': duration,
                        'file_size': file_size
                    }
                else:
                    print(f" ⚠️ Quality {quality} failed, trying next...")
                    
            except subprocess.TimeoutExpired:
                print(f" ⏰ Timeout at {quality} kbps, trying lower quality...")
                continue
            except Exception as e:
                print(f" ❌ Error at {quality}: {str(e)[:50]}")
                continue
        
        return {
            'success': False,
            'status': f'twitch_failed_all_qualities_{nickname}_duration_{duration}s'
        }

    def _try_get_recent_twitch_vod(self, videos_url: str, output_path: str, nickname: str, duration: int) -> Dict:
        """Get recent Twitch VOD with dynamic duration"""
        try:
            print(f" 🔍 Searching recent VODs for @{nickname}...")
            
            cmd = [
                'yt-dlp',
                '--dump-json',
                '--playlist-end', '1',
                '--quiet',
                '--no-warnings',
                videos_url
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0 and result.stdout.strip():
                lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
                if lines:
                    vod_info = json.loads(lines[0])
                    vod_url = vod_info.get('webpage_url') or vod_info.get('url')
                    vod_title = vod_info.get('title', 'Unknown Title')[:30]
                    
                    if vod_url:
                        print(f" 🎬 Found recent VOD: {vod_title}...")
                        return self._extract_twitch_sample(vod_url, output_path, nickname, duration)
            
            return {
                'success': False,
                'status': f'no_recent_vods_found_for_{nickname}'
            }
            
        except Exception as e:
            return {
                'success': False,
                'status': f'twitch_vod_search_failed_{nickname}: {str(e)[:100]}'
            }

    def _extract_best_username(self, link_data: Dict, url: str) -> str:
        """Extract username with URL parsing priority"""
        # Priority 1: Extract from URL
        username_from_url = self._extract_username_from_url(url)
        if username_from_url and len(username_from_url) > 2:
            return username_from_url
        
        # Priority 2: Real username fields
        username_fields = ['username', 'screen_name', 'user_name', 'handle']
        for field in username_fields:
            value = link_data.get(field)
            if value and not self._is_empty_value(value):
                username = str(value).strip()
                if username and not self._is_descriptive_text(username):
                    return username
        
        # Priority 3: Generate ID from URL
        if url:
            url_hash = abs(hash(url)) % 10000
            return f"user_{url_hash}"
        
        return f"user_{int(time.time()) % 10000}"

    def _extract_username_from_url(self, url: str) -> Optional[str]:
        """Extract username from YouTube or Twitch URL"""
        if not url:
            return None
            
        try:
            if 'youtube.com' in url or 'youtu.be' in url:
                patterns = [
                    r'/channel/([^/?]+)',
                    r'/user/([^/?]+)',
                    r'/c/([^/?]+)',
                    r'/@([^/?]+)',
                    r'/watch\?v=([^&]+)',
                    r'youtu\.be/([^/?]+)'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, url)
                    if match:
                        username = match.group(1)[:20]
                        if not username.startswith('UC'):
                            return username
                        return f"yt_{username[-8:]}"
                        
            elif 'twitch.tv' in url:
                patterns = [
                    r'twitch\.tv/([^/?]+)',
                    r'twitch\.tv/([^/]+)/videos'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, url)
                    if match:
                        username = match.group(1)
                        if username.lower() not in ['videos', 'clips', 'collections']:
                            return username[:20]
                            
        except Exception:
            pass
            
        return None

    def _is_empty_value(self, value) -> bool:
        """Check if value is empty"""
        if value is None or pd.isna(value):
            return True
        return str(value).lower().strip() in ['nan', '', 'none', 'null']

    def _is_descriptive_text(self, text: str) -> bool:
        """Check if text is descriptive rather than username"""
        if not text or len(text) > 30:
            return True
            
        descriptive_words = [
            'check', 'pinned', 'moved', 'see', 'bio', 'link', 'follow', 'subscribe'
        ]
        
        text_lower = text.lower()
        word_count = sum(1 for w in descriptive_words if w in text_lower)
        
        return word_count >= 1 or text.count(' ') >= 2

    def _sanitize_filename(self, filename: str) -> str:
        """Clean filename for safe file system usage"""
        if not filename or self._is_empty_value(filename):
            return f"user_{int(time.time()) % 10000}"
            
        # Remove special characters and emojis
        filename = re.sub(r'[^\w\s-]', '', str(filename), flags=re.UNICODE)
        filename = filename.lower()
        filename = re.sub(r'\s+', '_', filename)
        filename = re.sub(r'_+', '_', filename).strip('_')
        filename = re.sub(r'[^a-zA-Z0-9_]', '', filename)
        
        if len(filename) > 20:
            filename = filename[:20]
            
        if not filename or len(filename) < 2:
            return f"user_{int(time.time()) % 10000}"
            
        return filename

    def _print_extraction_summary(self, extracted_samples: List[Dict], total_links: int):
        """Print comprehensive extraction summary"""
        successful = len(extracted_samples)
        failed = total_links - successful
        
        print(f"\n🎤 ENHANCED VOICE SAMPLE EXTRACTION COMPLETED!")
        print("=" * 60)
        print(f"📊 Total links processed: {total_links}")
        print(f"✅ Successful extractions: {successful}")
        print(f"❌ Failed extractions: {failed}")
        print(f"📈 Success rate: {(successful / total_links * 100):.1f}%")
        print(f"📁 Samples saved in: {self.output_dir}")
        
        if extracted_samples:
            # Duration statistics
            durations = [sample.get('actual_duration', 0) for sample in extracted_samples]
            total_audio_time = sum(durations)
            avg_duration = total_audio_time / len(durations)
            
            print(f"\n📊 DURATION STATISTICS:")
            print(f" ⏱️ Total audio extracted: {total_audio_time} seconds ({total_audio_time/3600:.1f} hours)")
            print(f" 📊 Average sample duration: {avg_duration:.1f} seconds")
            print(f" ⏰ Shortest sample: {min(durations)} seconds")
            print(f" ⏰ Longest sample: {max(durations)} seconds")
            
            # Platform breakdown
            platforms = {}
            for sample in extracted_samples:
                platform = sample.get('platform_source', 'unknown')
                platforms[platform] = platforms.get(platform, 0) + 1
                
            print(f"\n🔗 PLATFORM BREAKDOWN:")
            for platform, count in platforms.items():
                print(f" {platform}: {count} samples")

    def generate_samples_report(self, extracted_samples: List[Dict], output_file: str = None) -> str:
        """Generate comprehensive report with duration analysis"""
        if not output_file:
            output_file = os.path.join(self.output_dir, "enhanced_voice_samples_report.txt")
            
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("🎤 ENHANCED VOICE SAMPLES EXTRACTION REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total samples extracted: {len(extracted_samples)}\n")
            f.write(f"Duration range: {self.min_duration}s - {self.max_duration}s\n")
            f.write(f"Strategy: Extract maximum available audio up to 1 hour\n")
            f.write(f"Audio quality: {self.quality} kbps\n")
            f.write(f"Output directory: {self.output_dir}\n\n")
            
            if extracted_samples:
                durations = [sample.get('actual_duration', 0) for sample in extracted_samples]
                total_time = sum(durations)
                
                f.write("📊 DURATION STATISTICS:\n")
                f.write(f"Total audio time: {total_time} seconds ({total_time/3600:.2f} hours)\n")
                f.write(f"Average duration: {total_time/len(durations):.1f} seconds\n")
                f.write(f"Shortest sample: {min(durations)} seconds\n")
                f.write(f"Longest sample: {max(durations)} seconds\n\n")
                
                f.write("📋 DETAILED SAMPLE LIST:\n")
                f.write("-" * 40 + "\n")
                
                for i, sample in enumerate(extracted_samples, 1):
                    f.write(f"{i:2d}. {sample.get('sample_filename', 'N/A')}\n")
                    f.write(f"    User: @{sample.get('processed_username', 'unknown')}\n")
                    f.write(f"    Platform: {sample.get('platform_source', 'unknown')}\n")
                    f.write(f"    Duration: {sample.get('actual_duration', 0)} seconds\n")
                    f.write(f"    File size: {sample.get('file_size', 0)//1000}KB\n")
                    f.write(f"    URL: {sample.get('url', 'N/A')[:50]}...\n\n")
        
        print(f"📄 Enhanced voice samples report saved: {output_file}")
        return output_file
