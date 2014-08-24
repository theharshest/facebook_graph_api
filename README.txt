This script lets user download the Facebook pages' posts in a range of dates provided and store the results to MySql database.
Before using this script make sure you have provided the correct credentials for the MySql connection in line 156.

Script takes the following inputs -

Enter App ID: developers.facebook.com -> Apps -> <app_name> -> App ID
Enter App Secret: developers.facebook.com -> Apps -> <app_name> -> App Secret
Enter User Access Token:  developers.facebook.com -> Tools -> Graph Explorer -> Select <app_name> from "Application" drop down -> Access Token
Enter page url (avoid trailing /): URL of any Facebook page (eg. https://www.facebook.com/walmart)
Enter start date(yyyy-mm-dd): Start date of the posts to download
Enter end date(yyyy-mm-dd): End date of the posts to download
