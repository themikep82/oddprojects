class Player:
	def __init__(self, argName, argKills, argDeaths, argAssists, argCreepScore):
		self.Name = argName
		self.Kills = argKills
		self.Deaths = argDeaths
		self.Assists = argAssists
		self.CreepScore = argCreepScore
		print (self.Name,"score:",self.getPlayerScore())
		
	def getPlayerScore(self):
		#print (self.CreepScore * .01)
		return (self.Kills * 3) + (self.Assists * 2) + (self.Deaths * -1) + (self.CreepScore * .01)
		
	
class Team:
	def __init__(self, argName, argTowerKills, argDragonKills, argBaronKills):
		self.Name = argName
		self.BaronKills = argBaronKills
		self.DragonKills = argDragonKills
		self.TowerKills = argTowerKills
		print (self.Name,"score:",self.getTeamScore())
		
	def getTeamScore(self):
		return (self.TowerKills * 1) + (self.DragonKills * 2) + (self.BaronKills * 3)
	
		
class UserRoster:
	def __init__(self, argTeam, argFlex, argSupport, argADC, argMid, argJungle, argTop):
		self.Top = argTop
		self.Mid = argMid
		self.Support = argSupport
		self.ADC = argADC
		self.Jungler = argJungle
		self.Flex = argFlex
		self.Team = argTeam

	def calculateTotalScore(self):
		return self.Team.getTeamScore() + self.Flex.getPlayerScore() + self.Support.getPlayerScore() + self.ADC.getPlayerScore() + self.Mid.getPlayerScore() + self.Jungler.getPlayerScore() + self.Top.getPlayerScore()
		

def rosterIsTied(userRosterA, userRosterB):
        
	if userRosterA.calculateTotalScore() == userRosterB.calculateTotalScore():
		return 'true'
	else:
		return 'false'
		
def rosterIsTiedRounded(userRosterA, userRosterB):
        
	if round(userRosterA.calculateTotalScore(), 2) == round(userRosterB.calculateTotalScore(), 2):
		return 'true'
	else:
		return 'false'

def rosterIsTiedIntCast(userRosterA, userRosterB): #only works for positive precision errors
        
	if int(userRosterA.calculateTotalScore() * 100) == int(userRosterB.calculateTotalScore() * 100):
		return 'true'
	else:
		return 'false'

def rosterIsTiedWithAbs(userRosterA, userRosterB):
        
	if abs(userRosterA.calculateTotalScore() - userRosterB.calculateTotalScore()) < .0001:
		return 'true'
	else:
		return 'false'
		
def main():

        
	
	H2K = Team('H2K',11,3,1)
	Hjarnan = Player('Hjarnan', 6,1,7,323)
	#PromisQ = Player('PromisQ', 0,1,20,37)
	Tabzz = Player('Tabzz', 8,0,10,238)
	Froggen = Player('Froggen', 6, 3, 14, 328)
	FroggenAlt = Player('FroggenAlt', 6, 3, 14, 338)
	Dexter = Player('Dexter',9,0,11,123)
	DexterAlt = Player('DexterAlt',9,0,11,113)
	#Odoamne = Player('Odoamne',5,1,11,274)
	kaSing = Player('kaSing',4,1,13,23)
	Jwaow = Player('Jwaow', 3, 0, 16, 214)
	
	
	testRosterA = UserRoster(H2K, Hjarnan, kaSing, Tabzz, Froggen, Dexter, Jwaow)
	testRosterB = UserRoster(H2K, Hjarnan, kaSing, Tabzz, FroggenAlt, DexterAlt, Jwaow)
	
	
	DavvaBG = UserRoster(H2K, Tabzz, kaSing, Hjarnan, Froggen, Dexter, Jwaow)
	TsMety = UserRoster(H2K, Hjarnan, kaSing, Tabzz, Froggen, Dexter, Jwaow)

	print ("\nraw roster alt scores")
	print ("DavvaBG:", testRosterA.calculateTotalScore())
	print ("TsMety:", testRosterB.calculateTotalScore())
	print ("Is it tied? :", rosterIsTied(testRosterA, testRosterB))
	
	
	print ("\nraw roster scores")
	print ("DavvaBG:", DavvaBG.calculateTotalScore())
	print ("TsMety:", TsMety.calculateTotalScore())
	print ("Is it tied? :", rosterIsTied(DavvaBG, TsMety))
			
	print ("\nroster scores rounded")

	print ("DavvaBG:", round(DavvaBG.calculateTotalScore(), 2))
	print ("TsMety:", round(TsMety.calculateTotalScore(), 2))
	print ("Is it tied? :", rosterIsTiedRounded(DavvaBG, TsMety))

	print ("\nintegerized roster scores")
	
	print ("DavvaBG:", int(DavvaBG.calculateTotalScore() * 100))
	print ("TsMety:", int(TsMety.calculateTotalScore() * 100))
	print ("Is it tied? :", rosterIsTiedIntCast(DavvaBG, TsMety))	
	
	print ("\nraw roster scores checked with absolute value")
	
	print ("DavvaBG:", DavvaBG.calculateTotalScore())
	print ("TsMety:", TsMety.calculateTotalScore())
	print ("Is it tied? :", rosterIsTiedWithAbs(DavvaBG, TsMety))
'''
	for creep in range(0, 400):
		creepScore=creep*.01	
                print (creepScore)
		#if (creepScore - round(creepScore, 2)!=0):
			#print ((creepScore - round(creepScore, 2)))
		#	print (creep)

'''
main()
	
	
