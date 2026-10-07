import asyncio
from playwright.async_api import async_playwright

URL = "http://127.0.0.1:54134/browser-abort.html"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe", args=["--no-sandbox"])
        page = await browser.new_page()
        errors = []
        page.on("console", lambda message: print(f"console:{message.type}:{message.text}"))
        page.on("pageerror", lambda error: errors.append(str(error)))
        await page.goto(URL, wait_until="domcontentloaded")
        try:
            await page.wait_for_function("document.querySelector('#result')?.textContent !== 'running'", timeout=30000)
        except Exception:
            pass
        result = await page.locator("#result").inner_text()
        print(f"result:{result}")
        if errors:
            raise RuntimeError("; ".join(errors))
        await browser.close()

asyncio.run(main())
