UPDATE Persons
SET  Persons.PersonCityName=(SELECT AddressList.PostCode FROM AddressList
WHERE AddressList.PersonId = Persons.PersonId)

