import os
import pandas as pd
import argparse
import sys
from config import Config
from step1_validate_accounts import AccountValidator
from step2_bright_data_trigger import BrightDataTrigger
from step3_bright_data_download import BrightDataDownloader
from step4_audio_filter import AudioContentFilter
from step4_5_audio_detector import AudioContentDetector
from step5_voice_verification import VoiceContentVerifier
from step6_voice_sample_extractor import VoiceSampleExtractor
from step7_advanced_voice_processor import AdvancedVoiceProcessor
from step8_noise_reduction import NoiseReducer
from snapshot_manager import SnapshotManager

def main(input_file, force_recheck=False):
    """Main pipeline execution - Enhanced YouTube & Twitch Voice Content Pipeline (30s-1hr samples)"""
    cfg = Config()
    os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)
    
    print("🎙️ ENHANCED YOUTUBE & TWITCH VOICE CONTENT PIPELINE")
    print("=" * 60)
    print("🎯 Focus: YouTube and Twitch voice content extraction")
    print("🎤 Output: 30s-1hr voice samples with advanced processing")
    print("🔍 Stages: 8 comprehensive processing stages")
    print(f"⏱️ Sample range: {cfg.MIN_SAMPLE_DURATION}s - {cfg.MAX_SAMPLE_DURATION}s")

    # Stage 1: Account Validation with Persistent Logging
    print("\n✅ STAGE 1: Account Validation with Persistent Logging")
    print("-" * 60)
    log_file = os.path.join(cfg.OUTPUT_DIR, "processed_accounts.json")
    validator = AccountValidator(
        max_concurrent=cfg.MAX_CONCURRENT_VALIDATIONS,
        delay_min=cfg.VALIDATION_DELAY_MIN,
        delay_max=cfg.VALIDATION_DELAY_MAX,
        log_file=log_file
    )
    
    existing_accounts_file = os.path.join(cfg.OUTPUT_DIR, "1_existing_accounts.csv")
    valid_accounts = validator.validate_accounts_from_file(
        input_file, existing_accounts_file, force_recheck=force_recheck
    )
    
    if not valid_accounts:
        print("❌ No valid accounts found.")
        return

    # Stage 2: Bright Data Snapshot Management
    print("\n🚀 STAGE 2: Bright Data Snapshot Management")
    print("-" * 60)
    trigger = BrightDataTrigger(cfg.BRIGHT_DATA_API_TOKEN, cfg.BRIGHT_DATA_DATASET_ID)
    usernames = [acc['username'] for acc in valid_accounts]
    
    sm = SnapshotManager(cfg.OUTPUT_DIR)
    existing_snapshot = sm.get_reusable_snapshot(usernames)
    
    if existing_snapshot:
        print(f"🔄 Using existing snapshot: {existing_snapshot}")
        snapshot_id = existing_snapshot
    else:
        print(f"🆕 Creating new snapshot for {len(usernames)} usernames")
        snapshot_id = trigger.create_snapshot_from_usernames(usernames)
        if not snapshot_id:
            print("❌ Failed to create snapshot")
            return
        sm.register_snapshot(snapshot_id, valid_accounts)

    # Stage 3: Data Download & External Link Extraction
    print("\n⬇️ STAGE 3: Data Download & External Link Extraction")
    print("-" * 60)
    downloader = BrightDataDownloader(cfg.BRIGHT_DATA_API_TOKEN)
    profiles = downloader.wait_and_download_snapshot(snapshot_id, cfg.MAX_SNAPSHOT_WAIT)
    
    if not profiles:
        print("❌ Failed to download snapshot data")
        sm.update_snapshot_status(snapshot_id, "failed")
        return
    
    sm.update_snapshot_status(snapshot_id, "completed", profiles)
    profiles_file = os.path.join(cfg.OUTPUT_DIR, f"2_snapshot_{snapshot_id}_results.csv")
    pd.DataFrame(profiles).to_csv(profiles_file, index=False)
    print(f"📊 Saved {len(profiles)} profiles to: {profiles_file}")
    
    links = downloader.extract_external_links(profiles)
    if not links:
        print("🔗 No external links found in profiles")
        return
        
    links_file = os.path.join(cfg.OUTPUT_DIR, f"3_snapshot_{snapshot_id}_external_links.csv")
    pd.DataFrame(links).to_csv(links_file, index=False)
    print(f"🔗 Saved {len(links)} external links to: {links_file}")

    # Stage 4: YouTube & Twitch Audio Platform Filtering
    print("\n🎯 STAGE 4: YouTube & Twitch Audio Platform Filtering")
    print("-" * 60)
    audio_filter = AudioContentFilter()
    audio_links = audio_filter.filter_audio_links(links)
    
    if not audio_links:
        print("🔍 No YouTube or Twitch links found")
        return
        
    audio_file = os.path.join(cfg.OUTPUT_DIR, f"4_snapshot_{snapshot_id}_audio_links.csv")
    pd.DataFrame(audio_links).to_csv(audio_file, index=False)
    print(f"🎯 Found {len(audio_links)} YouTube/Twitch audio links!")

    # Stage 4.5: Audio Content Detection
    print("\n🎵 STAGE 4.5: YouTube & Twitch Audio Content Detection")
    print("-" * 60)
    audio_detector = AudioContentDetector(timeout=10)
    audio_detected_links = audio_detector.detect_audio_content(audio_links)
    
    if not audio_detected_links:
        print("🔍 No audio content detected")
        return
        
    audio_detected_file = os.path.join(cfg.OUTPUT_DIR, f"4_5_snapshot_{snapshot_id}_audio_detected.csv")
    pd.DataFrame(audio_detected_links).to_csv(audio_detected_file, index=False)
    print(f"🎵 Found {len(audio_detected_links)} links with actual audio content!")

    # Stage 5: Voice Content Verification
    print("\n🎙️ STAGE 5: YouTube & Twitch Voice Content Verification")
    print("-" * 60)
    voice_verifier = VoiceContentVerifier(timeout=15)
    verified_links = voice_verifier.verify_voice_content(audio_detected_links)
    
    verified_file = os.path.join(cfg.OUTPUT_DIR, f"5_snapshot_{snapshot_id}_verified_voice.csv")
    pd.DataFrame(verified_links).to_csv(verified_file, index=False)
    
    confirmed_voice = [link for link in verified_links if link.get('has_voice')]
    if confirmed_voice:
        confirmed_file = os.path.join(cfg.OUTPUT_DIR, f"5_snapshot_{snapshot_id}_confirmed_voice.csv")
        pd.DataFrame(confirmed_voice).to_csv(confirmed_file, index=False)
        print(f"🎙️ Found {len(confirmed_voice)} confirmed voice content links!")
    else:
        print("❌ No voice content confirmed after verification")
        confirmed_voice = []

    # Stage 6: Enhanced Voice Sample Extraction (30s-1hr samples)
    print("\n🎤 STAGE 6: Enhanced Voice Sample Extraction (30s-1hr samples)")
    print("-" * 60)
    
    if confirmed_voice:
        sample_extractor = VoiceSampleExtractor(
            output_dir=os.path.join(cfg.OUTPUT_DIR, "voice_samples"),
            min_duration=cfg.MIN_SAMPLE_DURATION,  # 30 seconds minimum
            max_duration=cfg.MAX_SAMPLE_DURATION,  # 1 hour maximum
            quality="192"
        )
        
        extracted_samples = sample_extractor.extract_voice_samples(confirmed_voice)
        
        if extracted_samples:
            extraction_file = os.path.join(cfg.OUTPUT_DIR, f"6_snapshot_{snapshot_id}_voice_samples.csv")
            pd.DataFrame(extracted_samples).to_csv(extraction_file, index=False)
            
            report_file = sample_extractor.generate_samples_report(extracted_samples)
            
            # Show enhanced summary
            durations = [sample.get('actual_duration', 0) for sample in extracted_samples]
            total_hours = sum(durations) / 3600
            
            print(f"\n🎤 Enhanced Voice Sample Extraction Summary:")
            print(f" 📊 Total voice links: {len(confirmed_voice)}")
            print(f" ✅ Successful extractions: {len(extracted_samples)}")
            print(f" ⏱️ Total audio extracted: {total_hours:.2f} hours")
            print(f" 📊 Average sample duration: {sum(durations)/len(durations):.1f} seconds")
            print(f" 📁 Samples directory: {sample_extractor.output_dir}")
            print(f" 📄 Report file: {report_file}")
        else:
            print("❌ No voice samples could be extracted")
            extracted_samples = []
    else:
        print("⏭️ Skipping voice sample extraction - no confirmed voice content")
        extracted_samples = []

    # Stage 8: Background Noise Reduction
    print("\n🎛️ STAGE 8: Background Noise Reduction")
    print("-" * 60)
    
    if extracted_samples:
        samples_dir = os.path.join(cfg.OUTPUT_DIR, "voice_samples")
        noise_reducer = NoiseReducer(
            output_dir=os.path.join(cfg.OUTPUT_DIR, "voice_analysis"),
            mode="quick",
            sample_rate=16000
        )
        
        nr_results = noise_reducer.process_directory(samples_dir)
        successful_denoising = sum(1 for r in nr_results if r.get('output_file'))
        
        print(f"✅ Noise reduction completed: {successful_denoising} files denoised")
        
        # Update sample paths to point to denoised files
        denoised_dir = os.path.join(cfg.OUTPUT_DIR, "voice_analysis", "denoised_audio")
        for sample in extracted_samples:
            orig_path = sample.get('sample_file', '')
            if orig_path:
                base_name = os.path.splitext(os.path.basename(orig_path))[0]
                denoised_path = os.path.join(denoised_dir, f"{base_name}_denoised.wav")
                if os.path.exists(denoised_path):
                    sample['sample_file'] = denoised_path
                    sample['is_denoised'] = True
    else:
        print("⏭️ Skipping noise reduction - no extracted samples")

    # Stage 7: Advanced Voice Processing (moved after noise reduction)
    print("\n🔍 STAGE 7: Advanced Voice Processing (Voice-Only Detection)")
    print("-" * 60)
    
    if extracted_samples:
        processor = AdvancedVoiceProcessor(
            output_dir=os.path.join(cfg.OUTPUT_DIR, "voice_analysis"),
            min_voice_confidence=0.6,
            voice_segment_min_length=2.0
        )
        
        # Create temporary directory for processing
        temp_audio_dir = os.path.join(cfg.OUTPUT_DIR, "temp_audio_for_processing")
        os.makedirs(temp_audio_dir, exist_ok=True)
        
        # Copy audio files to temp directory
        import shutil
        for sample in extracted_samples:
            sample_file = sample.get('sample_file')
            if sample_file and os.path.exists(sample_file):
                dest_file = os.path.join(temp_audio_dir, os.path.basename(sample_file))
                if not os.path.exists(dest_file):
                    shutil.copy2(sample_file, dest_file)
        
        voice_only_results = processor.process_audio_directory(temp_audio_dir)
        
        if voice_only_results:
            results_file = processor.save_results(voice_only_results)
            report_file = processor.generate_report(voice_only_results)
            
            voice_only_file = os.path.join(cfg.OUTPUT_DIR, f"7_snapshot_{snapshot_id}_voice_only.csv")
            simplified_results = []
            
            for result in voice_only_results:
                simplified_results.append({
                    'processed_username': result.get('username', 'unknown'),
                    'platform_source': result.get('platform', 'unknown'),
                    'voice_only_file': result.get('voice_only_file', ''),
                    'speech_text': result.get('speech_analysis', {}).get('combined_text', ''),
                    'voice_confidence': result.get('final_analysis', {}).get('final_confidence', 0),
                    'word_count': result.get('speech_analysis', {}).get('word_count', 0),
                    'voice_duration': result.get('voice_duration', 0)
                })
            
            pd.DataFrame(simplified_results).to_csv(voice_only_file, index=False)
            
            print(f"🔍 Advanced Voice Processing Summary:")
            print(f" 📊 Total audio samples: {len(extracted_samples)}")
            print(f" ✅ Voice-only samples: {len(voice_only_results)}")
            print(f" 📈 Voice detection rate: {(len(voice_only_results) / len(extracted_samples) * 100):.1f}%")
            voice_only_samples = simplified_results
        else:
            print("❌ No voice-only content found after filtering")
            voice_only_samples = []
        
        # Cleanup temp directory
        if os.path.exists(temp_audio_dir):
            shutil.rmtree(temp_audio_dir)
    else:
        print("⏭️ Skipping advanced voice processing - no audio samples")
        voice_only_samples = []

    # Final comprehensive summary
    print("\n🎉 ENHANCED PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"📊 Total accounts processed: {len(valid_accounts)}")
    print(f"🔗 External links found: {len(links)}")
    print(f"🎯 YouTube/Twitch links: {len(audio_links)}")
    print(f"🎙️ Voice content confirmed: {len(confirmed_voice)}")
    print(f"🎤 Voice samples extracted: {len(extracted_samples) if extracted_samples else 0}")
    print(f"✅ Voice-only samples (filtered): {len(voice_only_samples) if voice_only_samples else 0}")
    
    if extracted_samples:
        total_hours = sum(sample.get('actual_duration', 0) for sample in extracted_samples) / 3600
        print(f"⏱️ Total audio extracted: {total_hours:.2f} hours")
        
        duration_stats = [sample.get('actual_duration', 0) for sample in extracted_samples]
        print(f"📊 Duration range: {min(duration_stats)}s - {max(duration_stats)}s")
    
    print(f"🆔 Snapshot ID: {snapshot_id}")
    print(f"📁 Results saved in: {cfg.OUTPUT_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced YouTube & Twitch Voice Content Pipeline")
    parser.add_argument("input_file", help="Input file with usernames (CSV or TXT)")
    parser.add_argument("--force-recheck", action="store_true", help="Force recheck of all accounts")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)
    
    main(args.input_file, args.force_recheck)
