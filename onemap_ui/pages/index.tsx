import type { NextPage } from "next";
import { useRef } from "react";
import Map, { Layer, MapRef, Marker, Source } from "react-map-gl/maplibre";
import { useEffect, useState } from 'react';
import { ConfigProvider, Breadcrumb, Layout, Menu, theme, Button, Form, Input, DatePicker, Tag, Popover, Alert } from 'antd/lib';
const { Header, Content, Footer } = Layout;
import { CarOutlined, FlagOutlined } from '@ant-design/icons';
import moment from 'moment-timezone';

const items = Array.from({ length: 1 }).map((_, index) => ({
  key: String(index + 1),
  label: 'Carpark Availability',
}));

const IndexPage: NextPage = () => {
  // const {
  //   token: { colorBgContainer, borderRadiusLG },
  // } = theme.useToken();
  const [locations, setLocations] = useState([
    {
        "carpark_id": "AR2M",
        "area": "QUEENSTOWN",
        "address": "BLK 28 DOVER CRESCENT",
        "latitude": 1.304738893,
        "longitude": 103.7816745,
        "x_coordinate": 22252.486,
        "y_coordinate": 31896.9749,
        "total_lots": 696,
        "agency": "HDB",
        "dataset": "hdb",
        "distance": 1.5114173927,
        "total_time_in_min": 48,
        "total_distance_in_km": 3.93,
        "rate": 1.2,
        "predicted_availability": 199,
        "normalized_predicted_availability": 1.0,
        "normalized_total_distance_in_km": 1.0,
        "normalized_rate": 0.00012001200120012,
        "normalized_total_distance_in_km_inverse": 0.0,
        "normalized_rate_inverse": 0.9998799879987998,
        "recommendation_score": 0.6666259960396039,
        "rank": 1
    },
    {
        "carpark_id": "W0029",
        "area": "QUEENSTOWN",
        "address": "WEST COAST PARK 1 OFF STREET",
        "latitude": 1.289622809,
        "longitude": 103.7714261,
        "x_coordinate": 21111.8896,
        "y_coordinate": 30225.541,
        "total_lots": 69,
        "agency": "URA",
        "dataset": "lta",
        "distance": 0.6351689794,
        "total_time_in_min": 29,
        "total_distance_in_km": 2.34,
        "rate": 2.4,
        "predicted_availability": 52,
        "normalized_predicted_availability": 0.2613065326633166,
        "normalized_total_distance_in_km": 0.5954198473282443,
        "normalized_rate": 0.00024002400240024,
        "normalized_total_distance_in_km_inverse": 0.40458015267175573,
        "normalized_rate_inverse": 0.9997599759975998,
        "recommendation_score": 0.5552149985620036,
        "rank": 2
    },
    {
        "carpark_id": "P0106",
        "area": "QUEENSTOWN",
        "address": "PASIR PANJANG ROAD- CLEMENTI ROAD OFF ST",
        "latitude": 1.291597869,
        "longitude": 103.768125,
        "x_coordinate": 20744.5188,
        "y_coordinate": 30443.9423,
        "total_lots": 9,
        "agency": "URA",
        "dataset": "lta",
        "distance": 0.938677936,
        "total_time_in_min": 22,
        "total_distance_in_km": 1.79,
        "rate": 2.4,
        "predicted_availability": 22,
        "normalized_predicted_availability": 0.11055276381909548,
        "normalized_total_distance_in_km": 0.45547073791348597,
        "normalized_rate": 0.00024002400240024,
        "normalized_total_distance_in_km_inverse": 0.5445292620865141,
        "normalized_rate_inverse": 0.9997599759975998,
        "recommendation_score": 0.5516134490204025,
        "rank": 3
    },
    {
        "carpark_id": "AR1M",
        "area": "QUEENSTOWN",
        "address": "BLK 2A DOVER ROAD",
        "latitude": 1.302848684,
        "longitude": 103.7836668,
        "x_coordinate": 22474.205,
        "y_coordinate": 31687.9608,
        "total_lots": 196,
        "agency": "HDB",
        "dataset": "hdb",
        "distance": 1.4287765211,
        "total_time_in_min": 44,
        "total_distance_in_km": 3.63,
        "rate": 1.2,
        "predicted_availability": 108,
        "normalized_predicted_availability": 0.542713567839196,
        "normalized_total_distance_in_km": 0.9236641221374046,
        "normalized_rate": 0.00012001200120012,
        "normalized_total_distance_in_km_inverse": 0.07633587786259544,
        "normalized_rate_inverse": 0.9998799879987998,
        "recommendation_score": 0.5396426049237192,
        "rank": 4
    },
    {
        "carpark_id": "AR1L",
        "area": "QUEENSTOWN",
        "address": "3 AND 5 DOVER ROAD",
        "latitude": 1.303876719,
        "longitude": 103.7826318,
        "x_coordinate": 22359.0217,
        "y_coordinate": 31801.6379,
        "total_lots": 4,
        "agency": "HDB",
        "dataset": "hdb",
        "distance": 1.4689092265,
        "total_time_in_min": 46,
        "total_distance_in_km": 3.77,
        "rate": 9999.0,
        "predicted_availability": 1,
        "normalized_predicted_availability": 0.005025125628140704,
        "normalized_total_distance_in_km": 0.9592875318066157,
        "normalized_rate": 1.0,
        "normalized_total_distance_in_km_inverse": 0.040712468193384255,
        "normalized_rate_inverse": 0.0,
        "recommendation_score": 0.01524584936131038,
        "rank": 5
    }
]);
  const [destination, setDestination] = useState({
    "postal_code": 119615,
    "latitude": 1.29214851229716,
    "longitude": 103.776550885502
});
  const [initialValue, setInitialValue] = useState(
    {
      "postal_code": 119615,
      "prediction_timestamp": "2025-04-14 01:00:00" // todo: change to next hour of dataset?
    }
  );
  const [isLoading, setIsLoading] = useState(false);

  const mapRef = useRef<MapRef>(null);

  const flyTo = (coordinates: [number, number]): void => {
    const map = mapRef.current?.getMap();
    if (!map) return;

    map.flyTo({
      center: coordinates,
      essential: true,
      zoom: 14,
    });
  };

  useEffect(() => {
    console.log("useEffect")
    const sgtTime = moment.tz(initialValue.prediction_timestamp,'Asia/Singapore')
    setIsLoading(true);
    fetch('http://localhost:8000/recommendations', {
      method: "POST",
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        "postal_code": parseInt(initialValue.postal_code) ? parseInt(initialValue.postal_code) : 238801,
        "prediction_timestamp": sgtTime,
        // "prediction_timestamp": value.prediction_timestamp
      }),
    })
      .then((response) => response.json())
      .then((data) => {
        setLocations(data.result);
        setDestination(data.destination);
        setIsLoading(false);
      })
      .catch(e => {
        console.log(e);
        setIsLoading(false)
      });
  }, []);

  const onFinish = (values) => {
    console.log('Form onFinish:', values);
    console.log('raw timestamp:', values.prediction_timestamp)
    // adding back 8 hours to compensate for antd automagically converting to UTC
    const sgtTime = values.prediction_timestamp.add(8,'hour')
    console.log("sgt: " + sgtTime)
    setIsLoading(true);
    fetch('http://localhost:8000/recommendations', {
      method: "POST",
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        "postal_code": parseInt(values.postal_code) ? parseInt(values.postal_code) : 238801,
        "prediction_timestamp": sgtTime,
        // "prediction_timestamp": values.prediction_timestamp
      }),
    })
      .then((response) => response.json())
      .then((data) => {
        setLocations(data.result);
        setDestination(data.destination);
        setIsLoading(false);
      })
      .catch(e => {
        console.log(e);
        setIsLoading(false)
      });
  };
  const onFinishFailed = (errorInfo) => {
    console.log('Failed:', errorInfo);
  };

  return (
    <ConfigProvider
      theme={{
        // 1. Use dark algorithm
        algorithm: theme.darkAlgorithm,

        // 2. Combine dark algorithm and compact algorithm
        // algorithm: [theme.darkAlgorithm, theme.compactAlgorithm],
      }}
    >
      <Layout>
        <Header
          style={{
            position: 'sticky',
            top: 0,
            zIndex: 1,
            width: '100%',
            display: 'flex',
            alignItems: 'center',
          }}
        >
          <div className="demo-logo" />
          <Menu
            theme="dark"
            mode="horizontal"
            defaultSelectedKeys={['2']}
            items={items}
            style={{ flex: 1, minWidth: 0 }}
          />
        </Header>
        <Content style={{ padding: '0 48px' }}>
          <Breadcrumb style={{ margin: '16px 0' }}>
            {/* <Breadcrumb.Item>Home</Breadcrumb.Item>
          <Breadcrumb.Item>List</Breadcrumb.Item>
          <Breadcrumb.Item>App</Breadcrumb.Item> */}
          </Breadcrumb>
          <div
            style={{
              padding: 24,
              minHeight: 380,
              // background: colorBgContainer,
              // borderRadius: borderRadiusLG,
            }}
          >
            <Form
              name="basic"
              labelCol={{
                // span: 8,
              }}
              wrapperCol={{
                span: 16,
              }}
              style={{
                maxWidth: 600,
              }}
              initialValues={{
                remember: true,
              }}
              onFinish={onFinish}
              onFinishFailed={onFinishFailed}
              autoComplete="off"
            >
              <Form.Item
                label="Postal Code"
                name="postal_code"
                rules={[
                  {
                    required: true,
                    message: 'Please input your postal code!',
                  },
                ]}
              >
                <Input />
              </Form.Item>
              <Form.Item
                label="Prediction Timestamp"
                name="prediction_timestamp"
                rules={[
                  {
                    required: true,
                    message: 'Please input your predicted time!',
                  }
                ]}
              >
                <DatePicker
                  showTime
                  onChange={(value, dateString) => {
                    console.log('Selected Time: ', value);
                    console.log('Formatted Selected Time: ', dateString);
                    // setValue({ ...value, prediction_timestamp: dateString })
                  }}
                />
                </Form.Item>
              <Form.Item label={null}>
                <Button type="primary" htmlType="submit">
                  Submit
                </Button>
              </Form.Item>
            </Form>
            <>
              {isLoading? (
                <Alert message="Loading results..." type="info" />
              ) : null}
            </>
            <br></br>
            <Map
              ref={mapRef}
              maxBounds={[103.596, 1.1443, 104.1, 1.4835]}
              mapStyle="https://www.onemap.gov.sg/maps/json/raster/mbstyle/Night.json"
              style={{
                width: "90vw",
                height: "90vh",
              }}
            >
              <Marker
                key={destination.postal_code}
                latitude={destination.latitude}
                longitude={destination.longitude}
              >
                <Tag color="yellow"><FlagOutlined /> Destination Postal Code: {destination.postal_code}</Tag>
              </Marker>
              {locations.map((location) => (
                <Marker
                  key={location.address}
                  latitude={location.latitude}
                  longitude={location.longitude}
                >
                  <div
                    className="mrt-marker"
                    onClick={() => flyTo([location.longitude, location.latitude])}
                  >
                    <Popover content={(
                      <>
                        <p>Predicted Availability: {location.predicted_availability} lots</p>
                        <p>Distance to Destination: {location.total_distance_in_km} km</p>
                        <p>Estimated Rate (1 hour): ${location.rate}</p>
                        {/* <p>Recommendation score: {location.recommendation_score}</p> */}
                        <p>Recommendation Rank: {location.rank}</p>
                      </>
                    )} title="Information">
                      <Tag color={location.rank == 1 ? 'orange' : 'blue'}><CarOutlined /> {location.address} ({location.carpark_id})</Tag>
                    </Popover>
                  </div>
                </Marker>
              ))}
            </Map>
          </div>
        </Content>
        <Footer style={{ textAlign: 'center' }}>
          Ant Design ©{new Date().getFullYear()} Created by Ant UED
        </Footer>
      </Layout>
    </ConfigProvider>
  );
};

export default IndexPage;