"""
104人力銀行 RPA職缺爬蟲
爬取關鍵字包含 "RPA" 的職缺資訊
"""

import requests
import json
import time
import csv
from datetime import datetime


def fetch_jobs(keyword="RPA", max_pages=5):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.104.com.tw/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }

    edu_map = {1: "國中以下", 2: "高中職", 3: "專科", 4: "大學", 5: "碩士", 6: "博士"}
    job_type_map = {1: "全職", 2: "兼職", 3: "高階", 4: "派遣", 5: "實習"}

    all_jobs = []

    for page in range(1, max_pages + 1):
        url = "https://www.104.com.tw/jobs/search/api/jobs"
        params = {
            "ro": 0, "kwop": 7, "keyword": keyword,
            "order": 15, "asc": 0, "s9": 1,
            "page": page, "mode": "s", "jobsource": "index_s",
        }

        try:
            print(f"正在爬取第 {page} 頁...")
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            # data["data"] 直接是職缺列表
            jobs = data.get("data", [])
            metadata = data.get("metadata", {})
            total = metadata.get("matchedCount", 0) if isinstance(metadata, dict) else 0

            if not jobs:
                print(f"第 {page} 頁無資料，停止爬取")
                break

            for job in jobs:
                # 薪資
                salary_low = job.get("salaryLow", 0)
                salary_high = job.get("salaryHigh", 0)
                if salary_high == 9999999:
                    salary_desc = f"面議（底薪 {salary_low:,} 起）" if salary_low else "面議"
                elif salary_low and salary_high:
                    salary_desc = f"{salary_low:,} ~ {salary_high:,} 元"
                else:
                    salary_desc = "面議"

                # 學歷
                edu_list = job.get("optionEdu", [])
                edu_desc = "、".join([edu_map.get(e, str(e)) for e in edu_list]) if edu_list else "不限"

                # 連結
                link_obj = job.get("link", {})
                job_link = link_obj.get("job", f"https://www.104.com.tw/job/{job.get('jobNo', '')}")
                cust_link = link_obj.get("cust", f"https://www.104.com.tw/company/{job.get('custNo', '')}")

                # 日期格式化
                appear_date = job.get("appearDate", "")
                if len(appear_date) == 8:
                    appear_date = f"{appear_date[:4]}-{appear_date[4:6]}-{appear_date[6:]}"

                job_info = {
                    "職缺名稱": job.get("jobName", ""),
                    "公司名稱": job.get("custName", ""),
                    "工作地點": (job.get("jobAddrNoDesc", "") + " " + job.get("jobAddress", "")).strip(),
                    "薪資": salary_desc,
                    "學歷要求": edu_desc,
                    "工作性質": job_type_map.get(job.get("jobType", 0), "不限"),
                    "遠端工作": "是" if job.get("remoteWorkType", 0) else "否",
                    "更新日期": appear_date,
                    "應徵人數": job.get("applyCnt", 0),
                    "工作描述": job.get("descSnippet", "").replace("\n", " "),
                    "產業類別": job.get("coIndustryDesc", ""),
                    "職缺連結": job_link,
                    "公司連結": cust_link,
                }
                all_jobs.append(job_info)

            print(f"  ✓ 第 {page} 頁完成，本頁 {len(jobs)} 筆" + (f"，共約 {total} 筆職缺" if total else ""))

            if total and len(all_jobs) >= total:
                print("已爬取所有職缺，結束")
                break

            time.sleep(1.5)

        except requests.exceptions.RequestException as e:
            print(f"第 {page} 頁請求失敗: {e}")
            break
        except (json.JSONDecodeError, KeyError) as e:
            print(f"第 {page} 頁資料解析失敗: {e}")
            break

    return all_jobs


def save_to_csv(jobs, filename=None):
    if not jobs:
        print("沒有資料可儲存")
        return None
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"104_RPA_jobs_{timestamp}.csv"
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=jobs[0].keys())
        writer.writeheader()
        writer.writerows(jobs)
    print(f"✅ CSV 已儲存至: {filename}")
    return filename


def save_to_json(jobs, filename=None):
    if not jobs:
        print("沒有資料可儲存")
        return None
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"104_RPA_jobs_{timestamp}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON 已儲存至: {filename}")
    return filename


def print_summary(jobs):
    if not jobs:
        print("未找到任何職缺")
        return
    print(f"\n{'='*60}")
    print(f"  爬取結果摘要：共 {len(jobs)} 筆 RPA 相關職缺")
    print(f"{'='*60}")
    print("\n【前5筆職缺預覽】")
    for i, job in enumerate(jobs[:5], 1):
        print(f"\n{i}. {job['職缺名稱']}")
        print(f"   公司: {job['公司名稱']}（{job['產業類別']}）")
        print(f"   地點: {job['工作地點']}")
        print(f"   薪資: {job['薪資']}")
        print(f"   學歷: {job['學歷要求']} | 遠端: {job['遠端工作']}")
        print(f"   更新: {job['更新日期']} | 應徵人數: {job['應徵人數']}")
        print(f"   連結: {job['職缺連結']}")
    print(f"\n{'='*60}")


def main():
    print("=" * 60)
    print("  104人力銀行 RPA職缺爬蟲")
    print("=" * 60)
    print(f"  搜尋關鍵字: RPA")
    print(f"  開始時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    jobs = fetch_jobs(keyword="RPA", max_pages=5)
    print_summary(jobs)

    if jobs:
        save_to_csv(jobs, filename="rpa_jobs_latest.csv")
        save_to_json(jobs)

    print(f"\n完成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":

    main()
