# Made by @codexnano from scratch.
# If you find any bugs, please let us know in the channel updates.
# You can 'git pull' to stay updated with the latest changes.

import logging, asyncio, aiohttp, aiofiles, zipfile, os
from pathlib import Path
from io import BytesIO
from PIL import Image
from config import Config
from services.util import sanitize, extract_chap_no, format_filename, DEF_FNAME
from services.catbox import Catbox
from urllib.parse import quote_plus
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
try:
    from reportlab.pdfgen import canvas
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False
log = logging.getLogger(__name__)
NANOBRIDGE_PROXY = "https://nanobridge.nanobridge-proxy.workers.dev/proxy"
PROTECTED_DOMAINS = [
    "wowpic", "asurascans", "asuratoon", "asura.gg",
    "asurascan", "imgbox", "cdn.asura"
]
def needs_proxy(url: str) -> bool:
    url_lower = url.lower()
    return any(domain in url_lower for domain in PROTECTED_DOMAINS)
class DL:
    def __init__(self):
        self.cfg = Config
        self.scraper = None
    async def __aenter__(self):
        self.sess = aiohttp.ClientSession(headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/avif,image/webp,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        })
        if HAS_CLOUDSCRAPER:
            self.scraper = cloudscraper.create_scraper()
        return self
    async def __aexit__(self, *args):
        if self.sess:
            await self.sess.close()
        if self.scraper:
            self.scraper.close()
    def _is_valid_image(self, data):
        if not data or len(data) < 10:
            return False
        if data[:3] == b'\xff\xd8\xff':
            return True
        if data[:4] == b'\x89PNG':
            return True
        if data[:4] == b'GIF8':
            return True
        if data[:4] == b'RIFF' and data[8:12] == b'WEBP':
            return True
        if data[:5] in (b'<!DOC', b'<html', b'<HTML', b'<?xml', b'error'):
            return False
        return any(b < 32 and b not in (9, 10, 13) for b in data[:20])
    async def img(self, url, path, base_url=None, max_retries=3, wmark_path=None, quality=None):
        # Check if file exists with any common image extension
        for e in [".jpg", ".png", ".webp", ".jpeg"]:
            if os.path.exists(str(path) + e):
                return True
        last_err = None
        use_proxy = needs_proxy(url)
        for attempt in range(max_retries):
            try:
                data = None
                if self.scraper and use_proxy:
                    try:
                        def _sync_dl():
                            with self.scraper.get(url, timeout=15, stream=True) as resp:
                                resp.raise_for_status()
                                return resp.content
                        data = await asyncio.to_thread(_sync_dl)
                        if data and self._is_valid_image(data):
                            pass
                        else:
                            data = None
                    except Exception as ce:
                        log.debug(f"[DL] Cloudscraper error: {ce}")
                        data = None
                if not data and use_proxy:
                    proxy_url = f"{NANOBRIDGE_PROXY}?url={quote_plus(url)}"
                    referer = base_url or "https://asuracomic.net/"
                    headers = {
                        "Referer": referer,
                        "Origin": referer.rstrip("/")
                    }
                    try:
                        async with self.sess.get(proxy_url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
                            if r.status == 200:
                                data = await r.read()
                                if not self._is_valid_image(data):
                                    log.warning(f"[DL] Proxy returned non-image data")
                                    data = None
                    except Exception as pe:
                        log.warning(f"[DL] Proxy error: {pe}")
                if not data:
                    headers = {}
                    url_lower = url.lower()
                    if "mangabuddy" in url_lower or "mbcdn" in url_lower or any(d in url_lower for d in ['.mbwmv.', '.mbuuj.', '.mbwnp.', '.mbegu.', '.mbwsp.', '.mbwbm.', '.mbbia.']):
                        headers["Referer"] = base_url or "https://mangabuddy.com/"
                    elif "flame" in url_lower:
                        headers["Referer"] = base_url or "https://flamecomics.com/"
                    elif "wowpic" in url_lower or "asura" in url_lower:
                        headers["Referer"] = base_url or "https://asuracomic.net/"
                    elif base_url:
                        headers["Referer"] = base_url
                    try:
                        async with self.sess.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
                            if r.status == 200:
                                data = await r.read()
                                if not self._is_valid_image(data):
                                    last_err = "Invalid image data"
                                    data = None
                            elif r.status in (521, 503, 502, 504):
                                last_err = f"HTTP {r.status} (server issue)"
                                await asyncio.sleep(2 * (attempt + 1))
                                continue
                            else:
                                last_err = f"HTTP {r.status}"
                    except asyncio.TimeoutError:
                        last_err = "Timeout"
                    except Exception as e:
                        last_err = str(e)[:50]
                if not data:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                def _process_img(img_data, save_path, w_path):
                    try:
                        with Image.open(BytesIO(img_data)) as img:
                            orig_format = img.format
                            ext = "." + (orig_format.lower() if orig_format else "jpg")
                            if ext == ".mpo": ext = ".jpg"
                            if ext == ".jpeg": ext = ".jpg"
                            
                            final_path = str(save_path) + ext
                            
                            # Check if resizing is needed
                            max_width = Config.MAX_IMAGE_WIDTH
                            needs_resize = img.width > max_width
                            needs_watermark = w_path and os.path.exists(w_path)

                            # If no processing is needed and it's already a good format, save raw
                            if not needs_resize and not needs_watermark and orig_format in ["JPEG", "MPO", "PNG", "WEBP"]:
                                with open(final_path, 'wb') as f:
                                    f.write(img_data)
                                return True

                            # Process image
                            img = img.convert("RGB")
                            if needs_resize:
                                ratio = max_width / img.width
                                img = img.resize((max_width, int(img.height * ratio)), Image.Resampling.LANCZOS)
                            
                            if needs_watermark:
                                try:
                                    with Image.open(w_path) as wm:
                                        wm_w = int(img.width * 0.20)
                                        wm_ratio = wm_w / wm.width
                                        wm_h = int(wm.height * wm_ratio)
                                        wm = wm.resize((wm_w, wm_h), Image.Resampling.LANCZOS)
                                        pos = (img.width - wm.width - 10, img.height - wm.height - 10)
                                        mask = wm if wm.mode == 'RGBA' else None
                                        img.paste(wm, pos, mask)
                                except Exception as wm_e:
                                    log.warning(f"[DL] Watermark apply fail: {wm_e}")

                            # Save as JPEG (we use .jpg for all processed images for simplicity)
                            final_path = str(save_path) + ".jpg"
                            qual = quality or Config.JPEG_QUALITY
                            img.save(
                                final_path, 
                                "JPEG", 
                                quality=qual, 
                                optimize=True, 
                                progressive=True,
                                subsampling=0 if qual > 90 else 422
                            )
                        return True
                    except Exception as ex:
                        log.warning(f"[DL] Processing fail for {save_path}: {ex}")
                        with open(str(save_path) + ".jpg", 'wb') as f:
                            f.write(img_data)
                        return True
                return await asyncio.to_thread(_process_img, data, path, wmark_path)
            except Exception as e:
                last_err = str(e)
                await asyncio.sleep(0.5 * (attempt + 1))
        log.error(f"[DL] IMG fail after {max_retries} retries: {url[:60]}... - {last_err}")
        return False
    async def get_imgs(self, urls, dir, base_url=None, wmark_path=None, quality=None):
        dir = Path(dir.parent) / sanitize(dir.name)
        log.info(f"[DL] Downloading {len(urls)} images to {dir}")
        try:
            dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            log.error(f"[DL] Failed to create dir {dir}: {e}")
            return False
        semaphore = asyncio.Semaphore(4)
        async def dl_with_sem(idx, url):
            async with semaphore:
                return await self.img(url, dir / f"{idx:03d}", base_url, wmark_path=wmark_path, quality=quality)
        tasks = [dl_with_sem(i, u) for i, u in enumerate(urls, 1)]
        res = await asyncio.gather(*tasks)
        import gc
        gc.collect()
        success = sum(res)
        failed = len(res) - success
        log.info(f"[DL] Downloaded {success}/{len(urls)} images, failed: {failed}")
        return success >= 1
    def _save_pdf_reportlab(self, image_paths, output_path):
        import gc
        
        # Determine target width for consistency (max among all images)
        target_width = 0
        for img_path in image_paths:
            try:
                with Image.open(img_path) as img:
                    if img.width > target_width:
                        target_width = img.width
            except:
                continue
        
        if target_width == 0:
            target_width = 800
        
        # Cap width to prevent massive PDF file size
        limit = Config.MAX_IMAGE_WIDTH
        if target_width > limit:
            target_width = limit
            
        log.info(f"[DL] Generating PDF with consistent width: {target_width}")
            
        # Write directly to file instead of BytesIO buffer to save RAM
        c = canvas.Canvas(str(output_path))
        for img_path in image_paths:
            try:
                # Get dimensions
                with Image.open(img_path) as img:
                    w, h = img.size
                
                # Align width and scale height proportionally
                ratio = target_width / w
                target_h = h * ratio
                
                c.setPageSize((target_width, target_h))
                # ReportLab handles the file reading/embedding efficiently
                c.drawImage(str(img_path), 0, 0, width=target_width, height=target_h, preserveAspectRatio=True)
                c.showPage()
                # Occasional GC during large PDF creation
                if len(image_paths) > 20: 
                    gc.collect()
            except Exception as e:
                log.warning(f"[DL] ReportLab page error for {img_path}: {e}")
        
        c.save()
        return output_path
    def _save_pdf_pillow(self, image_paths, output_path, qual=95):
        import gc
        if not image_paths:
            return None
        
        # Determine target width (max among all images)
        target_width = 0
        for pth in image_paths:
            try:
                with Image.open(pth) as img:
                    if img.width > target_width:
                        target_width = img.width
            except:
                continue

        if target_width == 0:
            target_width = 800

        limit = Config.MAX_IMAGE_WIDTH
        if target_width > limit:
            target_width = limit

        log.info(f"[DL] Creating PDF (Pillow) with consistent width: {target_width}")

        processed = []
        img1 = None
        
        for pth in image_paths:
            try:
                img = Image.open(pth).convert('RGB')
                if img.width != target_width:
                    ratio = target_width / img.width
                    new_h = int(img.height * ratio)
                    img = img.resize((target_width, new_h), Image.Resampling.LANCZOS)
                
                if img1 is None:
                    img1 = img
                else:
                    processed.append(img)
            except Exception as e:
                log.warning(f"[DL] Pillow page error: {e}")

        if not img1:
            return None

        try:
            img1.save(output_path, "PDF", save_all=True, append_images=processed, quality=qual)
        finally:
            img1.close()
            for p in processed:
                p.close()
            processed.clear()
            gc.collect()
        return output_path
    def pdf(self, dir, name, chap, qual=95, fname_fmt=None, first_promo=None, last_promo=None):
        try:
            chap_no = extract_chap_no(chap)
            fmt = fname_fmt or DEF_FNAME
            fname = format_filename(fmt, name, chap, chap_no)
            p = dir.parent / f"{fname}.pdf"
            p.parent.mkdir(parents=True, exist_ok=True)
            log.info(f"[DL] Creating PDF: {p}")
            imgs = sorted([f for f in dir.iterdir() if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".webp", ".mpo"]])
            if not imgs:
                log.warning(f"[DL] No images found in {dir}")
                return None
            all_pages = []
            if first_promo and Path(first_promo).exists():
                log.info("[DL] Adding first promo image")
                all_pages.append(Path(first_promo))
            all_pages.extend(imgs)
            if last_promo and Path(last_promo).exists():
                log.info("[DL] Adding last promo image")
                all_pages.append(Path(last_promo))
            if HAS_REPORTLAB:
                self._save_pdf_reportlab(all_pages, p)
            else:
                self._save_pdf_pillow(all_pages, p, qual)
            log.info(f"[DL] PDF created: {p.name} ({len(all_pages)} pages)")
            return p
        except Exception as e:
            log.error(f"[DL] PDF fail: {e}")
        return None
    def cbz(self, dir, name, chap, fname_fmt=None, first_promo=None, last_promo=None):
        try:
            chap_no = extract_chap_no(chap)
            fmt = fname_fmt or DEF_FNAME
            fname = format_filename(fmt, name, chap, chap_no)
            p = dir.parent / f"{fname}.cbz"
            log.info(f"[DL] Creating CBZ: {p}")
            imgs = sorted([f for f in dir.iterdir() if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".webp", ".mpo"]])
            if not imgs:
                log.warning(f"[DL] No images found in {dir}")
                return None
            with zipfile.ZipFile(p, 'w', zipfile.ZIP_DEFLATED) as z:
                page_num = 0
                if first_promo and Path(first_promo).exists():
                    z.write(first_promo, arcname=f"{page_num:03d}_promo_first.jpg")
                    page_num += 1
                for f in imgs:
                    z.write(f, arcname=f"{page_num:03d}_{f.name}")
                    page_num += 1
                if last_promo and Path(last_promo).exists():
                    z.write(last_promo, arcname=f"{page_num:03d}_promo_last.jpg")
            log.info(f"[DL] CBZ created: {p.name}")
            return p
        except Exception as e:
            log.error(f"[DL] CBZ fail: {e}")
            return None
    async def make(self, dir, name, chap, type='pdf', qual=95, fname_fmt=None, first_data=None, last_data=None):
        import base64
        first_promo = None
        last_promo = None
        temp_dir = dir.parent / "_temp_promo"
        temp_dir.mkdir(exist_ok=True)
        async def save_promo(data, filename):
            dest = temp_dir / filename
            if isinstance(data, tuple):
                dtype, dval = data
                if dtype == 'b64' and dval:
                    img_bytes = base64.b64decode(dval)
                    with open(dest, 'wb') as f:
                        f.write(img_bytes)
                    return dest
                elif dtype == 'url' and dval:
                    await Catbox.download(dval, str(dest), session=self.sess)
                    if dest.exists():
                        return dest
            elif isinstance(data, str) and data:
                await Catbox.download(data, str(dest), session=self.sess)
                if dest.exists():
                    return dest
            return None
        first_promo = await save_promo(first_data, "first.jpg")
        last_promo = await save_promo(last_data, "last.jpg")
        loop = asyncio.get_running_loop()
        if type == 'cbz':
            result = await loop.run_in_executor(None, self.cbz, dir, name, chap, fname_fmt, str(first_promo) if first_promo else None, str(last_promo) if last_promo else None)
        else:
            result = await loop.run_in_executor(None, self.pdf, dir, name, chap, qual, fname_fmt, str(first_promo) if first_promo else None, str(last_promo) if last_promo else None)
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        return result
