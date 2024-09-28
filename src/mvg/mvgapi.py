"""Provides the class MvgApi."""

from __future__ import annotations

import asyncio
import re
from enum import Enum
from typing import Any, List, Dict, Optional

import aiohttp
from furl import furl

MVGAPI_DEFAULT_LIMIT = 10  # API defaults to 10, limits to 100


class Base(Enum):
    """MVG APIs base URLs."""

    FIB = "https://www.mvg.de/api/fib/v3"
    ZDM = "https://www.mvg.de/.rest/zdm"


class Endpoint(Enum):
    """MVG API endpoints with URLs and arguments."""

    FIB_LOCATION = ("/location", ["query"])
    FIB_NEARBY = ("/station/nearby", ["latitude", "longitude"])
    FIB_DEPARTURE = ("/departure", ["globalId", "limit", "offsetInMinutes", "transportTypes"])
    FIB_LINE_STATION = ("/line/station", ["stationId"])  # New endpoint for lines per station
    ZDM_STATION_IDS = ("/mvgStationGlobalIds", [])
    ZDM_STATIONS = ("/stations", [])
    ZDM_LINES = ("/lines", [])
    MESSAGE = ("/message", [])  # Corrected endpoint for messages


class TransportType(Enum):
    """MVG products defined by the API with name and icon."""

    BAHN: tuple[str, str] = ("Bahn", "mdi:train")
    SBAHN: tuple[str, str] = ("S-Bahn", "mdi:subway-variant")
    UBAHN: tuple[str, str] = ("U-Bahn", "mdi:subway")
    TRAM: tuple[str, str] = ("Tram", "mdi:tram")
    BUS: tuple[str, str] = ("Bus", "mdi:bus")
    REGIONAL_BUS: tuple[str, str] = ("Regionalbus", "mdi:bus")
    SEV: tuple[str, str] = ("SEV", "mdi:taxi")
    SCHIFF: tuple[str, str] = ("Schiff", "mdi:ferry")

    @classmethod
    def all(cls) -> List[TransportType]:
        """Return a list of all products except SEV."""
        return [member for name, member in cls.__members__.items() if name != "SEV"]


class MvgApiError(Exception):
    """Failed communication with MVG API."""


class MvgApi:
    """A class interface to retrieve stations, lines, and departures from the MVG.

    The implementation uses the Münchner Verkehrsgesellschaft (MVG) API at https://www.mvg.de.
    It can be instantiated by station name and place or global station id.

    :param station: name, place ('Universität, München') or global station id (e.g. 'de:09162:70')
    :raises MvgApiError: raised on communication failure or unexpected result
    :raises ValueError: raised on bad station id format
    """

    def __init__(self, station: str) -> None:
        """Initialize the MVG interface."""
        station = station.strip()
        if self.valid_station_id(station):
            self.station_id = station
        else:
            station_details = self.station_query_sync(station)  # Use synchronous wrapper
            if station_details:
                self.station_id = station_details["id"]
            else:
                raise ValueError("Invalid station name or ID.")

    @staticmethod
    def valid_station_id(station_id: str, validate_existence: bool = False) -> bool:
        """
        Check if the station id is a global station ID according to VDV Recommendation 432.

        :param station_id: a global station id (e.g. 'de:09162:70')
        :param validate_existence: validate the existence in a list from the API
        :return: True if valid, False if Invalid
        """
        valid_format = bool(re.fullmatch(r"de:\d{2,5}:\d+", station_id))
        if not valid_format:
            return False

        if validate_existence:
            try:
                result = asyncio.run(MvgApi.__api(Base.ZDM, Endpoint.ZDM_STATION_IDS))
                assert isinstance(result, list)
                return station_id in result
            except (AssertionError, KeyError, MvgApiError):
                raise MvgApiError("Bad API call: Could not parse station data")

        return True

    @staticmethod
    async def __api(
            base: Base,
            endpoint: Endpoint,
            args: Optional[Dict[str, Any]] = None,
            path_param: Optional[str] = None,
    ) -> Any:
        """
        Call the API endpoint with the given arguments.

        :param base: the API base
        :param endpoint: the endpoint
        :param args: a dictionary containing arguments
        :param path_param: additional path parameter if needed (e.g., station_id for line/station)
        :raises MvgApiError: raised on communication failure or unexpected result
        :return: the response as JSON object
        """
        url = furl(base.value)
        endpoint_path, _ = endpoint.value
        if path_param:
            url /= f"{endpoint_path}/{path_param}"
        else:
            url /= endpoint_path

        if args:
            url.set(query_params=args)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        url.url, headers={"Accept": "application/json, text/plain, */*"}
                ) as resp:
                    if resp.status != 200:
                        raise MvgApiError(
                            f"Bad API call: Got response ({resp.status}) from {url.url}"
                        )
                    if resp.content_type != "application/json":
                        raise MvgApiError(
                            f"Bad API call: Got content type {resp.content_type} from {url.url}"
                        )
                    return await resp.json()

        except aiohttp.ClientError as exc:
            raise MvgApiError(f"Bad API call: Got {str(type(exc))} from {url.url}") from exc

    @staticmethod
    async def station_query(query: str) -> Optional[Dict[str, Any]]:
        """
        Find a station by station name and place.

        :param query: name, place (e.g., 'Hauptbahnhof, München')
        :raises MvgApiError: raised on communication failure or unexpected result
        :return: the first matching station as dictionary with keys 'id', 'name', 'place', 'latitude', 'longitude'
        """
        query = query.strip()
        try:
            args = {"query": query}
            result = await MvgApi.__api(Base.FIB, Endpoint.FIB_LOCATION, args)
            assert isinstance(result, list)

            # Return None if result is empty
            if not result:
                return None

            # Return the first station type entry
            for location in result:
                if location.get("type") == "STATION":
                    station = {
                        "id": location.get("globalId", ""),
                        "name": location.get("name", ""),
                        "place": location.get("place", ""),
                        "latitude": location.get("latitude", 0.0),
                        "longitude": location.get("longitude", 0.0),
                    }
                    return station

            # Return None if no station was found
            return None

        except (AssertionError, KeyError) as exc:
            raise MvgApiError("Bad API call: Could not parse station data") from exc

    @staticmethod
    def station_query_sync(query: str) -> Optional[Dict[str, Any]]:
        """
        Synchronous wrapper for station_query.

        :param query: name, place (e.g., 'Hauptbahnhof, München')
        :return: the first matching station as dictionary or None
        """
        return asyncio.run(MvgApi.station_query(query))

    @staticmethod
    async def station_ids_async() -> List[str]:
        """
        Retrieve a list of all valid station ids.

        :raises MvgApiError: raised on communication failure or unexpected result
        :return: station ids as a list
        """
        try:
            result = await MvgApi.__api(Base.ZDM, Endpoint.ZDM_STATION_IDS)
            assert isinstance(result, list)
            return sorted(result)
        except (AssertionError, KeyError) as exc:
            raise MvgApiError("Bad API call: Could not parse station data") from exc

    @staticmethod
    def station_ids() -> List[str]:
        """
        Synchronous wrapper for station_ids_async.

        :return: station ids as a list
        """
        return asyncio.run(MvgApi.station_ids_async())

    @staticmethod
    async def stations_async() -> List[Dict[str, Any]]:
        """
        Retrieve a list of all stations.

        :raises MvgApiError: raised on communication failure or unexpected result
        :return: a list of stations as dictionaries
        """
        try:
            result = await MvgApi.__api(Base.ZDM, Endpoint.ZDM_STATIONS)
            assert isinstance(result, list)
            return result
        except (AssertionError, KeyError) as exc:
            raise MvgApiError("Bad API call: Could not parse station data") from exc

    @staticmethod
    def stations() -> List[Dict[str, Any]]:
        """
        Retrieve a list of all stations.

        :raises MvgApiError: raised on communication failure or unexpected result
        :return: a list of stations as dictionaries
        """
        return asyncio.run(MvgApi.stations_async())

    @staticmethod
    async def lines_async(station_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve a list of all lines.

        If `station_id` is provided, retrieves lines for that specific station.
        Otherwise, retrieves lines for all stations.

        :param station_id: Optional global station id (e.g., 'de:09162:70')
        :raises MvgApiError: raised on communication failure or unexpected result
        :return: a list of lines as dictionaries
        """
        try:
            if station_id:
                # Fetch lines for a specific station
                result = await MvgApi.__api(Base.FIB, Endpoint.FIB_LINE_STATION, path_param=station_id)
                assert isinstance(result, list)
                return result
            else:
                # Fetch lines for all stations
                station_ids = await MvgApi.station_ids_async()
                # To improve performance, fetch lines concurrently
                tasks = [
                    MvgApi.__api(Base.FIB, Endpoint.FIB_LINE_STATION, path_param=station_id)
                    for station_id in station_ids
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                all_lines = []
                for res in results:
                    if isinstance(res, list):
                        all_lines.extend(res)
                    elif isinstance(res, Exception):
                        # Log or handle exceptions as needed
                        continue
                # Remove duplicates based on 'label' and 'transportType'
                unique_lines = {}
                for line in all_lines:
                    key = (line.get("label"), line.get("transportType"))
                    if key not in unique_lines:
                        unique_lines[key] = line
                return list(unique_lines.values())

        except (AssertionError, KeyError) as exc:
            raise MvgApiError("Bad API call: Could not parse lines data") from exc

    @staticmethod
    def lines(station_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve a list of all lines.

        If `station_id` is provided, retrieves lines for that specific station.
        Otherwise, retrieves lines for all stations.

        :param station_id: Optional global station id (e.g., 'de:09162:70')
        :return: a list of lines as dictionaries
        """
        return asyncio.run(MvgApi.lines_async(station_id))

    @staticmethod
    async def nearby_async(latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
        """
        Find the nearest station by coordinates.

        :param latitude: coordinate in decimal degrees
        :param longitude: coordinate in decimal degrees
        :raises MvgApiError: raised on communication failure or unexpected result
        :return: the first matching station as dictionary with keys 'id', 'name', 'place', 'latitude', 'longitude'

        Example result::

            {
                'id': 'de:09162:70',
                'name': 'Universität',
                'place': 'München',
                'latitude': 48.15007,
                'longitude': 11.581
            }
        """
        try:
            args = {
                "latitude": latitude,
                "longitude": longitude,
            }
            result = await MvgApi.__api(Base.FIB, Endpoint.FIB_NEARBY, args)
            assert isinstance(result, list)

            # Return the first station type entry
            for location in result:
                if location.get("type") == "STATION":
                    station = {
                        "id": location.get("globalId", ""),
                        "name": location.get("name", ""),
                        "place": location.get("place", ""),
                        "latitude": location.get("latitude", 0.0),
                        "longitude": location.get("longitude", 0.0),
                    }
                    return station

            # Return None if no station was found
            return None

        except (AssertionError, KeyError) as exc:
            raise MvgApiError("Bad API call: Could not parse nearby station data") from exc

    @staticmethod
    def nearby(latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
        """
        Find the nearest station by coordinates.

        :param latitude: coordinate in decimal degrees
        :param longitude: coordinate in decimal degrees
        :raises MvgApiError: raised on communication failure or unexpected result
        :return: the first matching station as dictionary with keys 'id', 'name', 'place', 'latitude', 'longitude'

        Example result::

            {
                'id': 'de:09162:70',
                'name': 'Universität',
                'place': 'München',
                'latitude': 48.15007,
                'longitude': 11.581
            }
        """
        return asyncio.run(MvgApi.nearby_async(latitude, longitude))

    @staticmethod
    async def departures_async(
            station_id: str,
            limit: int = MVGAPI_DEFAULT_LIMIT,
            offset: int = 0,
            transport_types: Optional[List[TransportType]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve the next departures for a station by station id.

        :param station_id: the global station id ('de:09162:70')
        :param limit: limit of departures, defaults to 10
        :param offset: offset (e.g., walking distance to the station) in minutes, defaults to 0
        :param transport_types: filter by transport type, defaults to None
        :raises MvgApiError: raised on communication failure or unexpected result
        :raises ValueError: raised on bad station id format
        :return: a list of departures as dictionaries

        Example result::

            [{
                'time': 1668524580,
                'planned': 1668524460,
                'line': 'U3',
                'destination': 'Fürstenried West',
                'type': 'U-Bahn',
                'icon': 'mdi:subway',
                'cancelled': False,
                'messages': []
            }, ... ]
        """
        station_id = station_id.strip()
        if not MvgApi.valid_station_id(station_id):
            raise ValueError("Invalid format of global station id.")

        try:
            args = {
                "globalId": station_id,
                "offsetInMinutes": offset,
                "limit": limit,
            }
            if transport_types is None:
                transport_types = TransportType.all()
            args["transportTypes"] = ",".join([product.name for product in transport_types])
            result = await MvgApi.__api(Base.FIB, Endpoint.FIB_DEPARTURE, args)
            assert isinstance(result, list)

            departures: List[Dict[str, Any]] = []
            for departure in result:
                transport_type = departure.get("transportType", "BAHN")
                # Handle unexpected transport types gracefully
                if transport_type not in TransportType.__members__:
                    transport_type = "BAHN"  # Default transport type

                departures.append(
                    {
                        "time": int(departure.get("realtimeDepartureTime", 0) / 1000),
                        "planned": int(departure.get("plannedDepartureTime", 0) / 1000),
                        "line": departure.get("label", ""),
                        "destination": departure.get("destination", ""),
                        "type": TransportType[transport_type].value[0],
                        "icon": TransportType[transport_type].value[1],
                        "cancelled": departure.get("cancelled", False),
                        "messages": departure.get("messages", []),
                    }
                )
            return departures

        except (AssertionError, KeyError) as exc:
            raise MvgApiError("Bad MVG API call: Invalid departure data") from exc

    def departures(
            self,
            limit: int = MVGAPI_DEFAULT_LIMIT,
            offset: int = 0,
            transport_types: Optional[List[TransportType]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve the next departures.

        :param limit: limit of departures, defaults to 10
        :param offset: offset (e.g., walking distance to the station) in minutes, defaults to 0
        :param transport_types: filter by transport type, defaults to None
        :raises MvgApiError: raised on communication failure or unexpected result
        :return: a list of departures as dictionaries

        Example result::

            [{
                'time': 1668524580,
                'planned': 1668524460,
                'line': 'U3',
                'destination': 'Fürstenried West',
                'type': 'U-Bahn',
                'icon': 'mdi:subway',
                'cancelled': False,
                'messages': []
            }, ... ]
        """
        return asyncio.run(
            MvgApi.departures_async(
                station_id=self.station_id,
                limit=limit,
                offset=offset,
                transport_types=transport_types,
            )
        )

    @staticmethod
    async def messages_async() -> List[Dict[str, Any]]:
        """
        Retrieve messages from the MVG.

        :raises MvgApiError: raised on communication failure or unexpected result
        :return: a list of messages as dictionaries
        """
        try:
            result = await MvgApi.__api(Base.FIB, Endpoint.MESSAGE)
            assert isinstance(result, list)
            return result
        except (AssertionError, KeyError) as exc:
            raise MvgApiError("Bad API call: Could not parse messages data") from exc

    @staticmethod
    def messages() -> List[Dict[str, Any]]:
        """
        Retrieve messages from the MVG.

        :raises MvgApiError: raised on communication failure or unexpected result
        :return: a list of messages as dictionaries
        """
        return asyncio.run(MvgApi.messages_async())
