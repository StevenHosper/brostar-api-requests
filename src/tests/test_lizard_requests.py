from unittest.mock import Mock, patch

from ..brostar_api_requests.lizard_requests import setup_lizard_session

# class TestUpdateLizard:
#     """Test suite for update_lizard function"""

#     @patch('brostar_api_requests.lizard_requests.setup_lizard_session')
#     def test_update_lizard_success(self, mock_setup_session):
#         """Test successful update of lizard location"""
#         # Arrange
#         mock_session = Mock()
#         mock_setup_session.return_value = mock_session

#         # Mock GET response
#         mock_get_response = Mock()
#         mock_get_response.json.return_value = {
#             'results': [{
#                 'url': 'https://vitens.lizard.net/api/v4/locations/123/',
#                 'extra_metadata': {'existing_key': 'existing_value'}
#             }]
#         }
#         mock_session.get.return_value = mock_get_response

#         # Mock PATCH response
#         mock_patch_response = Mock()
#         mock_patch_response.status_code = 200
#         mock_session.patch.return_value = mock_patch_response

#         # Act
#         result = update_lizard('TEST001', {'new_key': 'new_value'})

#         # Assert
#         mock_session.get.assert_called_once_with(
#             'https://vitens.lizard.net/api/v4/locations/?code=TEST001'
#         )
#         mock_get_response.raise_for_status.assert_called_once()

#         mock_session.patch.assert_called_once_with(
#             'https://vitens.lizard.net/api/v4/locations/123/',
#             json={
#                 'extra_metadata': {
#                     'existing_key': 'existing_value',
#                     'new_key': 'new_value'
#                 }
#             }
#         )
#         mock_patch_response.raise_for_status.assert_called_once()
#         assert result == mock_patch_response

#     @patch('brostar_api_requests.lizard_requests.setup_lizard_session')
#     def test_update_lizard_merges_metadata(self, mock_setup_session):
#         """Test that existing metadata is preserved and merged"""
#         # Arrange
#         mock_session = Mock()
#         mock_setup_session.return_value = mock_session

#         existing_metadata = {
#             'key1': 'value1',
#             'key2': 'value2'
#         }

#         mock_get_response = Mock()
#         mock_get_response.json = Mock(return_value={  # Make json a Mock that returns a value
#             'results': [{
#                 'url': 'https://vitens.lizard.net/api/v4/locations/123/',
#                 'code': 'TEST001',
#                 'extra_metadata': existing_metadata.copy()
#             }]
#         })
#         mock_session.get.return_value = mock_get_response
#         mock_session.patch.return_value = Mock()

#         # Act
#         update_lizard('TEST001', {'key2': 'updated_value', 'key3': 'new_value'})

#         # Assert
#         call_args = mock_session.patch.call_args
#         expected_metadata = {
#             'key1': 'value1',
#             'key2': 'updated_value',  # Updated
#             'key3': 'new_value'       # New
#         }
#         assert call_args[1]['json']['extra_metadata'] == expected_metadata

#     @patch('brostar_api_requests.lizard_requests.setup_lizard_session')
#     def test_update_lizard_empty_existing_metadata(self, mock_setup_session):
#         """Test handling when extra_metadata doesn't exist"""
#         # Arrange
#         mock_session = Mock()
#         mock_setup_session.return_value = mock_session

#         mock_get_response = Mock()
#         mock_get_response.json.return_value = {
#             'results': [{
#                 'url': 'https://vitens.lizard.net/api/v4/locations/123/',
#                 'extra_metadata': {}
#             }]
#         }
#         mock_session.get.return_value = mock_get_response
#         mock_session.patch.return_value = Mock()

#         # Act
#         update_lizard('TEST001', {'new_key': 'new_value'})

#         # Assert
#         call_args = mock_session.patch.call_args
#         assert call_args[1]['json']['extra_metadata'] == {'new_key': 'new_value'}

#     @patch('brostar_api_requests.lizard_requests.logger')
#     @patch('brostar_api_requests.lizard_requests.setup_lizard_session')
#     def test_update_lizard_get_http_error(self, mock_setup_session, mock_logger):
#         """Test handling of HTTP error during GET request"""
#         # Arrange
#         mock_session = Mock()
#         mock_setup_session.return_value = mock_session

#         mock_get_response = Mock()
#         mock_get_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
#         mock_session.get.return_value = mock_get_response

#         # Act
#         result = update_lizard('NONEXISTENT', {'key': 'value'})

#         # Assert
#         assert result is None
#         mock_logger.error.assert_called_once()
#         assert 'Error fetching location NONEXISTENT' in mock_logger.error.call_args[0][0]
#         mock_session.patch.assert_not_called()

#     @patch('brostar_api_requests.lizard_requests.setup_lizard_session')
#     def test_update_lizard_patch_raises_http_error(self, mock_setup_session):
#         """Test that PATCH HTTPError is raised (not caught)"""
#         # Arrange
#         mock_session = Mock()
#         mock_setup_session.return_value = mock_session

#         mock_get_response = Mock()
#         mock_get_response.json.return_value = {
#             'results': [{
#                 'url': 'https://vitens.lizard.net/api/v4/locations/123/',
#                 'extra_metadata': {}
#             }]
#         }
#         mock_session.get.return_value = mock_get_response

#         mock_patch_response = Mock()
#         mock_patch_response.raise_for_status.side_effect = requests.exceptions.HTTPError("403 Forbidden")
#         mock_session.patch.return_value = mock_patch_response

#         # Act & Assert
#         with pytest.raises(requests.exceptions.HTTPError):
#             update_lizard('TEST001', {'key': 'value'})

#     @patch('brostar_api_requests.lizard_requests.setup_lizard_session')
#     def test_update_lizard_empty_results(self, mock_setup_session):
#         """Test handling when no results are returned"""
#         # Arrange
#         mock_session = Mock()
#         mock_setup_session.return_value = mock_session

#         mock_get_response = Mock()
#         mock_get_response.json.return_value = {'results': []}
#         mock_session.get.return_value = mock_get_response

#         # Act & Assert
#         with pytest.raises(IndexError):
#             update_lizard('TEST001', {'key': 'value'})


class TestSetupLizardSession:
    """Test suite for setup_lizard_session function"""

    @patch.dict("os.environ", {"LIZARD_API_KEY": "test_api_key_123"})
    @patch("brostar_api_requests.lizard_requests.requests.Session")
    def test_setup_lizard_session_creates_session(self, mock_session_class):
        """Test that session is created with correct configuration"""
        # Arrange
        mock_session_instance = Mock()
        mock_session_class.return_value = mock_session_instance

        # Act
        result = setup_lizard_session()

        # Assert
        assert result == mock_session_instance
        assert mock_session_instance.headers == {
            "username": "__key__",
            "password": "test_api_key_123",
            "Content-Type": "application/json",
        }

    # @patch.dict('os.environ', {'LIZARD_API_KEY': 'test_api_key_123'})
    # @patch('brostar_api_requests.lizard_requests.HTTPAdapter')
    # @patch('brostar_api_requests.lizard_requests.requests.Session')
    # def test_setup_lizard_session_mounts_adapter(self, mock_session_class, mock_adapter_class):
    #     """Test that HTTP adapter is mounted correctly"""
    #     # Arrange
    #     mock_session_instance = Mock()
    #     mock_session_class.return_value = mock_session_instance
    #     mock_adapter_instance = Mock()
    #     mock_adapter_class.return_value = mock_adapter_instance

    #     # Act
    #     setup_lizard_session()

    #     # Assert
    #     mock_adapter_class.assert_called_once()
    #     call_kwargs = mock_adapter_class.call_args[1]
    #     assert call_kwargs['pool_connections'] == 5
    #     assert call_kwargs['pool_maxsize'] == 5

    #     # Verify retry configuration
    #     retry_obj = call_kwargs['max_retries']
    #     assert retry_obj.total == 6
    #     assert retry_obj.backoff_factor == 0.5
    #     assert retry_obj.status_forcelist == [500, 502, 503, 504]

    #     # Verify adapter mounting
    #     assert mock_session_instance.mount.call_count == 2
    #     mock_session_instance.mount.assert_any_call("http://", mock_adapter_instance)
    #     mock_session_instance.mount.assert_any_call("https://", mock_adapter_instance)

    @patch.dict("os.environ", {}, clear=True)
    def test_setup_lizard_session_missing_api_key(self):
        """Test behavior when LIZARD_API_KEY is not set"""
        # Act
        result = setup_lizard_session()

        # Assert - Should still create session but with None password
        assert result.headers["password"] is None


# Integration-style test (optional, requires actual mocking of requests)
# class TestUpdateLizardIntegration:
#     """Integration tests with more realistic scenarios"""

#     @patch('brostar_api_requests.lizard_requests.setup_lizard_session')
#     def test_update_lizard_full_workflow(self, mock_setup_session):
#         """Test the complete workflow with realistic data"""
#         # Arrange
#         mock_session = Mock(spec=requests.Session)
#         mock_setup_session.return_value = mock_session

#         # Simulate realistic API responses
#         get_response = Mock()
#         get_response.json.return_value = {
#             'count': 1,
#             'results': [{
#                 'uuid': '123e4567-e89b-12d3-a456-426614174000',
#                 'code': 'GMN_001',
#                 'url': 'https://vitens.lizard.net/api/v4/locations/123e4567/',
#                 'extra_metadata': {
#                     'owner': 'Vitens',
#                     'installation_date': '2020-01-15'
#                 }
#             }]
#         }

#         patch_response = Mock()
#         patch_response.status_code = 200
#         patch_response.json.return_value = {
#             'extra_metadata': {
#                 'owner': 'Vitens',
#                 'installation_date': '2020-01-15',
#                 'last_maintenance': '2024-11-20'
#             }
#         }

#         mock_session.get.return_value = get_response
#         mock_session.patch.return_value = patch_response

#         # Act
#         result = update_lizard('GMN_001', {'last_maintenance': '2024-11-20'})

#         # Assert
#         assert result.status_code == 200
#         patched_data = mock_session.patch.call_args[1]['json']['extra_metadata']
#         assert patched_data['last_maintenance'] == '2024-11-20'
#         assert patched_data['owner'] == 'Vitens'
