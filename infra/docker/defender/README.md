# go-defender

Is a web-service which provides REST API interface to perform antivirus scanning against locally-saved files via `Windows Defender` service.

## An interface

1. `GET /ping` -- returns following JSON object which indicates either the service is ready to receive requests:
   ```json
      {
        "IsHealthy": "true"
      }
      ```
2. `POST /scan` -- allows to perform file scanning. Receives JSON body with scanning details:
   ```json
   {
     "TraceId": "c21a8443-c15c-4ea4-8d9b-1d222ba4dade",
     "FileName": "/files/file_to_scan.bin"
   }
   ```
   <details>
      <summary>Succeeded response</summary>

      ```json
      {
        "Output": "EngineScanCallback(): Threat TrojanDropper:Win32/Pykspa.A identified",
        "Error": null
      }
      ```
   </details>

   <details>
      <summary>Failed response</summary>

      ```json
      {
        "Output": "",
        "Error": "the antivirus service is not available"
      }
      ```
   </details>
3. `POST /scan-files` -- allows to perform bulk files scanning. Receives JSON body with scanning details:
   ```json
   {
     "FileNames": ["file_1.bin", "file_2.bin", "file_3.bin"],
     "TraceId": "c21a8443-c15c-4ea4-8d9b-1d222ba4dade"
   }
   ```
   <details>
      <summary>Succeeded response</summary>
   
      ```json
      {
          "Results": [
              {
                 "FileName": "file_1.bin",
                 "IsEncrypted": false,
                 "IsInfected": false,
                 "InfectionReason": null,
                 "Error": null
              },
              {
                 "FileName": "file_2.bin",
                 "IsEncrypted": true,
                 "IsInfected": false,
                 "InfectionReason": null,
                 "Error": null
              },
              {
                 "FileName": "file_3.bin",
                 "IsEncrypted": false,
                 "IsInfected": true,
                 "InfectionReason": "TrojanDropper:Win32/Pykspa.A",
                 "Error": null
              }
          ],
          "Error": ""
      }
      ```
   </details>

   <details>
      <summary>Failed response</summary>
   
      ```json
      {
        "Results": [],
        "Error": "the antivirus service is not available"
      }
      ```
   </details>
