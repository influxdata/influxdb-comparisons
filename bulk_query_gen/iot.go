package bulk_query_gen

// Devops describes a devops query generator.
type Iot interface {
	AverageTemperatureDayByHourOneHome(Query)

	Dispatch(int) Query
}

// devopsDispatchAll round-robins through the different devops queries.
func IotDispatchAll(d Iot, iteration int, q Query, scaleVar int) {
	if scaleVar <= 0 {
		panic("logic error: bad scalevar")
	}
	mod := 1
	if scaleVar >= 2 {
		mod++
	}
	if scaleVar >= 4 {
		mod++
	}
	if scaleVar >= 8 {
		mod++
	}
	if scaleVar >= 16 {
		mod++
	}
	if scaleVar >= 32 {
		mod++
	}

	switch iteration % mod {
	case 0:
		d.AverageTemperatureDayByHourOneHome(q)
	//case 1:
	//	d.MaxCPUUsageHourByMinuteTwoHosts(q, scaleVar)
	//case 2:
	//	d.MaxCPUUsageHourByMinuteFourHosts(q, scaleVar)
	//case 3:
	//	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	//case 4:
	//	d.MaxCPUUsageHourByMinuteSixteenHosts(q, scaleVar)
	//case 5:
	//	d.MaxCPUUsageHourByMinuteThirtyTwoHosts(q, scaleVar)
	default:
		panic("logic error in switch statement")
	}
}
