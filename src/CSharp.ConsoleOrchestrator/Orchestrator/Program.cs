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
            // fallback minimal prompt
            promptTemplate =
@"You are a strict JSON-only generator. Input: an array of question candidates.
For each candidate output an object with fields:
page (int), qnum (string or null), english (string), hindi (string), notes (string).
Return a JSON array only. No extra text.
<BATCH_JSON_HERE>";
        }

        // HTTP client for Ollama
        using var http = new HttpClient();
        http.Timeout = TimeSpan.FromMinutes(2);

        var finalQuestions = new List<JsonElement>();

        foreach (var kv in byPage)
        {
            int page = kv.Key;
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

            // Attempt to extract JSON array from response text.
            string jsonArrayText = ExtractJsonArray(respText);
            if (jsonArrayText == null)
            {
                Console.WriteLine("[WARN] Could not extract JSON array from Ollama response. Raw response below:");
                Console.WriteLine(respText);
                continue;
            }

            try
            {
                var parsed = JsonDocument.Parse(jsonArrayText);
                if (parsed.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in parsed.RootElement.EnumerateArray())
                    {
                        finalQuestions.Add(item.Clone());
                    }
                }
                else
                {
                    Console.WriteLine("[WARN] Parsed JSON is not an array. Skipping this batch.");
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
