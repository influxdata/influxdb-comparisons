package bulk_query_gen

// Devops describes a devops query generator.
type Devops interface {
	MaxCPUUsageHourByMinuteOneHost(Query)
	MaxCPUUsageHourByMinuteTwoHosts(Query)
	MaxCPUUsageHourByMinuteFourHosts(Query)
	MaxCPUUsageHourByMinuteEightHosts(Query)
	MaxCPUUsageHourByMinuteSixteenHosts(Query)
	MaxCPUUsageHourByMinuteThirtyTwoHosts(Query)

	MaxCPUUsage12HoursByMinuteOneHost(Query)

	MeanCPUUsageDayByHourAllHostsGroupbyHost(Query)

	//CountCPUUsageDayByHourAllHostsGroupbyHost(Query)

	Dispatch(int) Query
}

// devopsDispatchAll round-robins through the different devops queries.
func DevopsDispatchAll(d Devops, iteration int, q Query, scaleVar int) {
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
		d.MaxCPUUsageHourByMinuteOneHost(q)
	case 1:
		d.MaxCPUUsageHourByMinuteTwoHosts(q)
	case 2:
		d.MaxCPUUsageHourByMinuteFourHosts(q)
	case 3:
		d.MaxCPUUsageHourByMinuteEightHosts(q)
	case 4:
		d.MaxCPUUsageHourByMinuteSixteenHosts(q)
	case 5:
		d.MaxCPUUsageHourByMinuteThirtyTwoHosts(q)
	default:
		panic("logic error in switch statement")
	}
}
