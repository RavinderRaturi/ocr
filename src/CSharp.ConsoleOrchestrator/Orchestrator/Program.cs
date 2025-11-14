using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Collections.Generic;

class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: Orchestrator <path-to-sample_blocks.jsonl>");
            return 1;
        }

        // Resolve input paths
        var blocksPath = Path.GetFullPath(args[0]);
        if (!File.Exists(blocksPath))
        {
            Console.WriteLine("[ERROR] blocks file not found: " + blocksPath);
            return 2;
        }

        // Project directory is the working directory when dotnet run --project ... is used.
        var projectDir = Directory.GetCurrentDirectory();

        // Correct path to Python.OCRTools inside repo
        var pythonFolder = Path.GetFullPath(Path.Combine(projectDir, "src", "Python.OCRTools"));

        Console.WriteLine($"[Info] Current project dir: {projectDir}");
        Console.WriteLine($"[Info] Python folder: {pythonFolder}");


        if (!Directory.Exists(pythonFolder))
        {
            Console.WriteLine("[ERROR] Python.OCRTools folder not found at:");
            Console.WriteLine(pythonFolder);
            return 3;
        }

        // Output paths
        var candidatesPath = Path.Combine(Path.GetDirectoryName(blocksPath)!, "candidates.jsonl");
        var finalQuestionsPath = Path.Combine(Path.GetDirectoryName(blocksPath)!, "questions.json");

        // Prefer venv python.exe if present
        var venvPython = Path.Combine(pythonFolder, "venv", "Scripts", "python.exe");
        var pythonExe = File.Exists(venvPython) ? venvPython : "python";

        var mergeScript = Path.Combine(pythonFolder, "merge_candidates.py");
        if (!File.Exists(mergeScript))
        {
            Console.WriteLine("[ERROR] merge_candidates.py not found at:");
            Console.WriteLine(mergeScript);
            return 4;
        }

        Console.WriteLine($"[Info] Using python executable: {pythonExe}");
        Console.WriteLine($"[Info] Merge script: {mergeScript}");
        Console.WriteLine($"[Info] Blocks input: {blocksPath}");
        Console.WriteLine($"[Info] Candidates output: {candidatesPath}");

        // Run the Python merge script
        var mergeProc = new System.Diagnostics.Process();
        mergeProc.StartInfo.FileName = pythonExe;
        mergeProc.StartInfo.Arguments = $"\"{mergeScript}\" \"{blocksPath}\" \"{candidatesPath}\"";
        mergeProc.StartInfo.WorkingDirectory = pythonFolder;
        mergeProc.StartInfo.UseShellExecute = false;
        mergeProc.StartInfo.RedirectStandardOutput = true;
        mergeProc.StartInfo.RedirectStandardError = true;

        try
        {
            mergeProc.Start();
        }
        catch (Exception ex)
        {
            Console.WriteLine("[ERROR] Failed to start python process: " + ex.Message);
            return 5;
        }

        var mergeStdOut = mergeProc.StandardOutput.ReadToEnd();
        var mergeStdErr = mergeProc.StandardError.ReadToEnd();
        mergeProc.WaitForExit();

        if (mergeProc.ExitCode != 0)
        {
            Console.WriteLine("[ERROR] Merge script returned non-zero exit code.");
            Console.WriteLine(mergeStdErr);
            Console.WriteLine(mergeStdOut);
            return 6;
        }

        Console.WriteLine("[Info] Merge script output:");
        Console.WriteLine(mergeStdOut);

        // Read candidates
        if (!File.Exists(candidatesPath))
        {
            Console.WriteLine("[ERROR] candidates.jsonl not produced at: " + candidatesPath);
            return 7;
        }

        var candidates = new List<JsonElement>();
        foreach (var line in File.ReadAllLines(candidatesPath))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            try
            {
                var doc = JsonDocument.Parse(line);
                candidates.Add(doc.RootElement.Clone());
            }
            catch (Exception ex)
            {
                Console.WriteLine("[WARN] Failed to parse candidate line. Skipping. " + ex.Message);
            }
        }

        if (candidates.Count == 0)
        {
            Console.WriteLine("[ERROR] No candidates found in: " + candidatesPath);
            return 8;
        }

        // Group candidates by page
        var byPage = new SortedDictionary<int, List<JsonElement>>();
        foreach (var c in candidates)
        {
            int page = 1;
            try
            {
                if (c.TryGetProperty("page", out var pval) && pval.ValueKind == JsonValueKind.Number)
                    page = pval.GetInt32();
            }
            catch { /* keep page=1 if missing */ }

            if (!byPage.ContainsKey(page)) byPage[page] = new List<JsonElement>();
            byPage[page].Add(c);
        }

        // Load prompt template file from python folder
        var promptTemplatePath = Path.Combine(pythonFolder, "prompt_template.txt");
        string promptTemplate;
        if (File.Exists(promptTemplatePath))
        {
            promptTemplate = File.ReadAllText(promptTemplatePath);
        }
        else
        {
//            // fallback minimal prompt
//            promptTemplate =
//@"You are a strict JSON-only generator. Input: an array of question candidates.
//For each candidate output an object with fields:
//page (int), qnum (string or null), english (string), hindi (string), notes (string).
//Return a JSON array only. No extra text.
//<BATCH_JSON_HERE>";
              promptTemplate =
                @"You are a strict JSON-only generator. Input: an array of question candidate objects in this format:
{""page"": <int>, ""qnum"": <string|null>, ""blocks"":[ {""lang"":""english""|""hindi""|""mixed"",""text"":""..."",""bbox"":{...},""conf"":<int>} , ... ] }

Task. For each input candidate produce exactly one JSON object with fields:
- page (int)
- qnum (string|null)
- english (string)  // cleaned English text, empty string if none
- hindi (string)    // cleaned Hindi text, empty string if none
- notes (string)    // short note if uncertain, otherwise empty string

Return a JSON array only. No commentary. No markdown. No extra fields.

Example 1.
Input:
[{""page"":1,""qnum"":""1"",""blocks"":[{""lang"":""english"",""text"":""What is the capital of India?"",""conf"":95},{""lang"":""hindi"",""text"":""भारत की राजधानी क्या है?"",""conf"":92}]}]
Output:
[{""page"":1,""qnum"":""1"",""english"":""What is the capital of India?"",""hindi"":""भारत की राजधानी क्या है?"",""notes"":""""}]

Example 2.
Input:
[{""page"":1,""qnum"":null,""blocks"":[{""lang"":""mixed"",""text"":""1. The largest planet is? 1. सबसे बड़ा ग्रह कौन सा है?"",""conf"":85}]}]
Output:
[{""page"":1,""qnum"":""1"",""english"":""The largest planet is?"",""hindi"":""सबसे बड़ा ग्रह कौन सा है?"",""notes"":""qnum inferred from leading '1.'""}]

Now process the following input exactly and return a JSON array of objects as specified above.
<BATCH_JSON_HERE>

";
        }

        // HTTP client for Ollama
        using var http = new HttpClient();
        http.Timeout = TimeSpan.FromMinutes(2);

        var finalQuestions = new List<JsonElement>();

        foreach (var kv in byPage)
        {
            int page = kv.Key;
            // TEMP: process only page 1
            if (page != 1)
                continue;
            var batch = kv.Value;
            var batchJson = JsonSerializer.Serialize(batch, new JsonSerializerOptions { WriteIndented = false });

            var prompt = promptTemplate.Replace("<BATCH_JSON_HERE>", batchJson);

            var requestBody = new
            {
                model = "qwen:7b", // use Ollama model id format name:tag
                prompt = prompt,
                max_tokens = 2000
            };

            string requestJson = JsonSerializer.Serialize(requestBody);

            Console.WriteLine($"[Info] Sending page {page} batch to Ollama. Candidates: {batch.Count}");

            HttpResponseMessage resp;
            try
            {
                resp = await http.PostAsync("http://localhost:11434/api/generate",
                    new StringContent(requestJson, Encoding.UTF8, "application/json"));
            }
            catch (Exception ex)
            {
                Console.WriteLine("[ERROR] Ollama request failed: " + ex.Message);
                return 9;
            }

            if (!resp.IsSuccessStatusCode)
            {
                var errText = await resp.Content.ReadAsStringAsync();
                Console.WriteLine($"[ERROR] Ollama returned HTTP {(int)resp.StatusCode}: {resp.ReasonPhrase}");
                Console.WriteLine(errText);
                // continue to next page instead of aborting all
                continue;
            }

            var respText = await resp.Content.ReadAsStringAsync();

            // Save raw response for inspection
            try
            {
                var rawPath = Path.Combine(Path.GetDirectoryName(blocksPath)!, $"raw_response_page_{page}.txt");
                await File.WriteAllTextAsync(rawPath, respText, Encoding.UTF8);
                Console.WriteLine("[Info] Saved raw Ollama response to: " + rawPath);
            }
            catch (Exception ex)
            {
                Console.WriteLine("[WARN] Failed to write raw response: " + ex.Message);
            }


            // Attempt to extract JSON array from response text.
            string jsonArrayText = ExtractJsonArray(respText);
            // If extracted JSON looks like an array of primitives (numbers or strings), retry once
            bool looksLikePrimitiveArray = false;
            try
            {
                using var quickCheck = JsonDocument.Parse(jsonArrayText);
                if (quickCheck.RootElement.ValueKind == JsonValueKind.Array)
                {
                    bool allPrimitive = true;
                    foreach (var it in quickCheck.RootElement.EnumerateArray())
                    {
                        if (it.ValueKind == JsonValueKind.Object)
                        {
                            allPrimitive = false;
                            break;
                        }
                    }
                    looksLikePrimitiveArray = allPrimitive;
                }
            }
            catch
            {
                // ignore
            }

            if (looksLikePrimitiveArray)
            {
                Console.WriteLine("[WARN] Ollama returned primitive array, retrying with explicit correction instruction...");

                var retryPrompt =
                    "Your last output was an array of primitive values (numbers/strings). " +
                    "You must return a JSON ARRAY OF OBJECTS. Each object must contain these fields ONLY: " +
                    "page (int), qnum (string|null), english (string), hindi (string), notes (string). " +
                    "Do not return arrays of numbers. Do not return plain strings. " +
                    "Process the same input candidates again: " + batchJson;

                var retryBody = new
                {
                    model = "qwen:7b",
                    prompt = retryPrompt,
                    max_tokens = 2000
                };

                var retryResp = await http.PostAsync(
                    "http://localhost:11434/api/generate",
                    new StringContent(JsonSerializer.Serialize(retryBody), Encoding.UTF8, "application/json")
                );

                var retryText = await retryResp.Content.ReadAsStringAsync();

                // Save retry output for debugging
                try
                {
                    var retryPath = Path.Combine(Path.GetDirectoryName(blocksPath)!, $"raw_response_page_{page}_retry.txt");
                    await File.WriteAllTextAsync(retryPath, retryText, Encoding.UTF8);
                }
                catch { }

                var retryJsonArray = ExtractJsonArray(retryText);
                if (retryJsonArray != null)
                    jsonArrayText = retryJsonArray;
            }


            try
            {
                var parsed = JsonDocument.Parse(jsonArrayText);
                // after jsonArrayText obtained and parsed into parsedDoc
                if (parsed.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in parsed.RootElement.EnumerateArray())
                    {
                        // reject primitive items like "1" or 1
                        if (item.ValueKind != JsonValueKind.Object)
                        {
                            Console.WriteLine("[WARN] Skipping primitive model output item.");
                            continue;
                        }
                        // require english or hindi non-empty
                        string english = item.TryGetProperty("english", out var e) && e.ValueKind == JsonValueKind.String ? e.GetString() ?? "" : "";
                        string hindi = item.TryGetProperty("hindi", out var h) && h.ValueKind == JsonValueKind.String ? h.GetString() ?? "" : "";
                        if (string.IsNullOrWhiteSpace(english) && string.IsNullOrWhiteSpace(hindi))
                        {
                            Console.WriteLine("[WARN] Skipping empty question object from model.");
                            // optionally log raw item to failures folder
                            continue;
                        }
                        finalQuestions.Add(item.Clone());
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("[WARN] Failed to parse extracted JSON: " + ex.Message);
                Console.WriteLine("Extracted JSON:");
                Console.WriteLine(jsonArrayText);
            }
        }

        // Save final questions
        try
        {
            var options = new JsonSerializerOptions { WriteIndented = true };
            var finalJson = JsonSerializer.Serialize(finalQuestions, options);
            await File.WriteAllTextAsync(finalQuestionsPath, finalJson, Encoding.UTF8);
            Console.WriteLine($"[Info] Wrote {finalQuestions.Count} questions to {finalQuestionsPath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine("[ERROR] Failed to write questions file: " + ex.Message);
            return 10;
        }

        return 0;
    }

    // Helper to extract first JSON array found in text.
    static string? ExtractJsonArray(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return null;

        // Try direct parse first
        text = text.Trim();
        if (text.StartsWith("[") && text.EndsWith("]"))
        {
            return text;
        }

        // Find first '[' and matching closing ']' using stack to handle nested structures
        int firstIdx = text.IndexOf('[');
        if (firstIdx < 0) return null;

        int depth = 0;
        for (int i = firstIdx; i < text.Length; i++)
        {
            char c = text[i];
            if (c == '[') depth++;
            else if (c == ']')
            {
                depth--;
                if (depth == 0)
                {
                    int lastIdx = i;
                    var candidate = text.Substring(firstIdx, lastIdx - firstIdx + 1).Trim();
                    // Quick sanity check: ensure it contains at least one object marker
                    if (candidate.Contains("{") && candidate.Contains("}")) return candidate;
                    return candidate;
                }
            }
        }
        return null;
    }
}
