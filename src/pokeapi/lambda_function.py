import asyncio
import json

import aiohttp
import boto3


async def list_pokemon_urls(
    session: aiohttp.ClientSession, limit: int = 10_000
) -> list:
    """_summary_

    Parameters
    ----------
    session : aiohttp.ClientSession
        An aiohttp session that can be used for making async http calls
    linit : int
        How many pokemons to return.
        Defaults to 10.000 which loosly means every pokemon

    Returns
    -------
    list
        All pokemon urls found
    """
    url = f"https://pokeapi.co/api/v2/pokemon?limit={limit}&offset=0"

    async with session.get(url) as resp:
        found_pokemons = await resp.json()

    return [poke_url["url"] for poke_url in found_pokemons["results"]]


async def get_pokemon(session: aiohttp.ClientSession, url: str) -> dict:
    """
    Makes an http request towards the given url.

    Parameters
    ----------
    session : aiohttp.ClientSession
        An aiohttp session that can be used for making async http calls
    url : str
        the url that you want to call

    Returns
    -------
    dict
        Entire payload in raw form
    """
    async with session.get(url) as resp:
        pokemon = await resp.json()

        return pokemon


async def catch_all_pokemons() -> dict:
    """
    Wrapper function that 'catches' all pokemons

    Returns
    -------
    dict
        Info on all pokemons found
    """

    async with aiohttp.ClientSession() as session:
        poke_urls = await list_pokemon_urls(session, 10000)

        tasks = [asyncio.ensure_future(get_pokemon(session, url)) for url in poke_urls]

        pokemons = await asyncio.gather(*tasks)

        return pokemons


def respond() -> dict:
    """
    Creates a response for the lambda function

    Right now this is static because i have not implemented error handling

    Returns
    -------
    dict
        Response dict with statusCode, headers and body
    """
    return {
        "statusCode": "200",
        "body": "Success",
        "headers": {
            "Content-Type": "application/json",
        },
    }


def upload_to_s3(json_to_upload: dict, bucket_name: str):
    """
    Takes a dict of pokemons and uplodas to an s3 bucket.

    Because the file is later copied into Redshift with the COPY command
    the uploaded file is in a non-valid json format with one row per pokemon json.

    File will be uploaded to staging/pokemon/pokemons.json and overwrite the previous file

    Parameters
    ----------
    json_to_upload : dict
        The dict with pokemons that should be loaded to s3
    bucket_name : str
        The name of the s3 bucket
    """

    # The password in plain text is obviously a very bad idea...
    session = boto3.Session(
        aws_access_key_id="AKIAY2TFIG3UGBZVYS5Q",
        aws_secret_access_key="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",
    )
    s3 = session.resource("s3")

    with open("/tmp/pokemons.json", "w") as pokemon_file:
        for j in json_to_upload:
            pokemon_file.write(json.dumps(j))
            pokemon_file.write("\n")

    s3.meta.client.upload_file(
        "/tmp/pokemons.json", bucket_name, "staging/pokemon/pokemons.json"
    )


def lambda_handler(event=None, context=None):
    """
    Function to be executed in lambda
    """
    loop = asyncio.get_event_loop()

    pokemons = loop.run_until_complete(catch_all_pokemons())

    upload_to_s3(pokemons, "phil-lego-bucket")

    return respond()
