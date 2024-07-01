
### README: Configuring Access for Uploading Data to Hugging Face Datasets

#### Creating and Configuring Your Hugging Face Access Token

To upload datasets to Hugging Face, you'll need an access token with the appropriate permissions. Follow these steps to create and configure your token:

#### Step 1: Create a Hugging Face Account
If you haven't already, create an account at [Hugging Face's website](https://huggingface.co/join).

#### Step 2: Generate an Access Token
1. Log into your Hugging Face account.
2. Navigate to your account settings by clicking on your profile picture in the upper right corner, then select 'Settings'.
3. Go to the 'Access Tokens' section.
4. Click on 'New Token'.
5. Name your token and select the appropriate role. To upload datasets, choose the "Write" role which allows you to upload and modify datasets.
6. Click 'Create a token'.

#### Step 3: Configure the Token in Your Environment
1. Copy the generated token.
2. Open or create a `.env` file in the root directory of your project.
3. Add the following line to your `.env` file:

   ```
   HUGGINGFACE_TOKEN=<YOUR_HF_TOKEN_HERE>
   ```

   Replace `<YOUR_HF_TOKEN_HERE>` with the token you copied in the previous step.

#### Step 4: Utilize the Token in Your Software
Ensure that your software is configured to read the `HF_TOKEN` from the environment variables. This is typically handled in your Python script as follows:

```python
import os
from huggingface_hub import HfApi, HfFolder
from dotenv import load_dotenv

load_dotenv()
# Ensure the token is loaded from the .env file
api = HfApi(token=os.getenv('HUGGINGFACE_TOKEN'))
```

#### Finalizing Setup
After configuring the token in your `.env` file, your miner should be able to authenticate with Hugging Face and upload datasets without requiring further login steps.

### Additional Information
- Keep your token secure and do not share it publicly.
- If you need to regenerate your token, repeat the steps above to generate a new one and update your `.env` file accordingly.
